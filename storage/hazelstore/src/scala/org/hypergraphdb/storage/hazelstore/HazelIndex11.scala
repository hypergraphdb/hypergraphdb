package org.hypergraphdb.storage.hazelstore

import Callables.BidirCallables11.GetItFromThatMember
import Callables.Index11Callables.{RemoveAllEntriesMono, RemoveEntryMono, AddEntryMono}
import com.hazelcast.core._
import org.hypergraphdb._
import com.hazelcast.query.{Predicate, PredicateBuilder}
import collection.JavaConversions._


import scala.Serializable
import java.util
import storage.ByteArrayConverter
import storage.hazelstore.Common._
import storage.hazelstore.Callables.Index11Callables._
import util.AbstractMap.SimpleEntry
import util.Comparator
import util.concurrent.{TimeUnit, Callable}

class HazelIndex11[K, V] (val name: String,
                          val h:HazelcastInstance,
                         hstoreConf:HazelStoreConfig,
                         implicit val keyConverter:   ByteArrayConverter[K],
                         implicit val valueConverter: ByteArrayConverter[V],
                         val providedComparator: Comparator[Array[Byte]] = BAComparator)
  extends HGSortIndex[K, V] with Serializable {


  // Requirements of HGSortIndex and Implementation Details. (summary below)
  //  1) add/remove/removeAll entries, and also find &  findFirst
  //      => simple Hazelcast-MultiMap operations
  //      => which is here >> kvmm <<, mapping index keys to the wrapped byte array representation of index values
  //      Notes:  # kvmm does use as multimap key not the keys of type K itself, but 160 bit hash values (FiveInt) of their respective byte array representation.
  //              # Wrapping of byte arrays in BAW ("ByteArrayWrapper") is necessary since bare byte arrays don't have their equals/hashcode overriden.
  //              # Wrapping of byte arrays is only necessary for values in hazelcast maps/multimaps, not keys (Hazelcast calculates hashs itself on the basis of byte arrays)

  //  2) findGT/GTE/LT/LTE however require >range queries< on the key and retrieval of their associated values.
  //      => cannot be done with any mapping. In order for Hazelcast to allow that, it requires:
  //          => keys of this index (type K) to be in the >value< position of a regular map (not multimap),
  //          => definition of an ordered index on this map (see mapIndexConfig)
  //          => the value type be a comparable
  //      => >> keyMap <<
  //      Notes:  # keyMap doesn't use BAWs but ComparableBAW. Since values themselves are not given to be comparable, but a comparator is provided by HGDB, the ComparableBAW must also carry a serialized ByteComparator for each blob!!
  //                  => It's highly desirable to find a better solution for that!
  //              # although mapping is keyHash => keys (BAWs), it's actually used in the reverse way: hazelcast range queries return the keyHashes of matching keys

  //  3) count / count(key) are required to be constant time operations.
  //      => this requirement would not be given if kvmm.keySet.size  / kvmm.get(keyHash).size was used
  //      => >> valCountMap << is simply a separate Map that stores the count of values for each key (keyHash => Long)

  //  Furthermore
  //      A)   >> firstValMap << is a separate simple map to allow good performance of findFirst queries.
  //        # this was done because querying kvmm.get(keyHash).iterator is a very expensive operation (transfer of potentially big collection of blobs)
  //        # in previous Callable implementations, kvmm.get(keyHash).iterator seemed to cause Serialization problems
  //        # in asynchronuous mode, when callables are performed on nodes owning key, this maybe unnecessary. However, async mode is unstable, and transactional mode does not make use of distributed execution of PartitionAware-Callables
  //        # Downside: we store one value blob per key twice!
  //        # not strictly a requirement, and this maybe removed in the future in case async proves reliable

  //      B) Implementation approach:
  //        # Most methods are implemented in terms of specific Callable classes defined elsewhere.
  //        # These callables are instantiated and handled by the transact method in three modi
  //            i)    transactional: callable is executed as part of a HGTransaction, which is created using hazelstore.transactionfactory
  //            ii)   non-transactional-synchronous: executed as i, still HGTransactionManager handles execution of callable
  //            iii)  non-transactional-asynchronous: callables are excuted by Hazelcast.Executor
  //        # Instead of using references to IMap / MultiMap instances (kvmm, keyMap, valCountMap, firstValMap),
  //          these Callables are constructed with the name of these Maps (see fields "kvmmName", keyMapName, valCountMapName, firstValMapName).
  //          This avoids serialization exceptions and hence is necessary for asynchronous execution.
  //        # Note: in case asynchronous execution will never work reliably, it should be examined how callables should be defined with

  // SUMMARY

  // Main-Maps:
  // kvmm:        keyHash => value-Blobs    = MultiMap:   FiveInt => BAW
  // keymap:      keyHash => key-blobs      = Map:        FiveInt => ComparableBAW

  // Auxilliary:
  // valCountMap: keyhash => Long           = Map:        FiveInt => Long           (linear time count(key))
  // firstValMap: keyHash => value-Blob     = Map:        FiveInt => BAW
  // indexKeyCount:                         = AtomicNumber                          (linear time count())

 // val h = hstoreConf.getHazelcastInstance.getOrElse(Hazelcast.getDefaultInstance)

  /*--------------------------------------------------------------------------------*/
  val kvmmName                                    = name + "keyValueMultiMap"
  val kvmm:MultiMap[FiveInt, BAW]                                   = h.getMultiMap(kvmmName)
  /*--------------------------------------------------------------------------------*/
  val keyMapName: String                          = name + "keyMap"
/*  val mapIndexConfig                              = new com.hazelcast.config.MapIndexConfig("data", true)
  val keyMapConfig                                = new com.hazelcast.config.MapConfig(keyMapName)
  keyMapConfig.addMapIndexConfig(mapIndexConfig)
  h.getConfig.addMapConfig(keyMapConfig) */
  val keyMap: IMap[FiveInt, ComparableBAW]                              = h.getMap(keyMapName)

  /*--------------------------------------------------------------------------------*/

  /*--------------------------------------------------------------------------------*/
//  type Args       = (ComparableBAW, FiveInt, BAW)
  def eo                                          = new PredicateBuilder().getEntryObject
  /*--------------------------------------------------------------------------------*/
  type VCntMap                                    = IMap[FiveInt,Long]
  val firstValMapName                             = "firstvalMap" + name
  val firstValMap:IMap[FiveInt, BAW]              = h.getMap(firstValMapName)
  /*--------------------------------------------------------------------------------*/

  val valCountMapName                             = name + "valCountMap"
  val valCountMap:IMap[FiveInt, Long]             = h.getMap(valCountMapName)
  /*--------------------------------------------------------------------------------*/
  val indexKeyCountName                           = "keyCountOfIndex" + name
  def indexKeyCount                               = h.getAtomicNumber(indexKeyCountName)

  /*--------------------------------------------------------------------------------*/

  implicit val comparator:Comparator[Array[Byte]] =
                                 if(providedComparator == null) BAComparator
                                 else providedComparator

  val executor = h.getExecutorService

  def execute[T](callable: Callable[T], returns:Boolean = false):T =
    if (! callable.isInstanceOf[PartitionAware[FiveInt]])
      callable.call()
    else if (!hstoreConf.getAsync || returns)
      executor.submit(callable).get()              // get makes it block == synchronous, but still better to transfer one block of code over to one keyHash owner
    else
      { executor.submit(callable); Unit}.asInstanceOf[T]


  def addEntry(key: K, value: V) {
    if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA) = initialVals(key, Option(value))
      execute(new AddEntryMono(keyMapName, kvmmName,firstValMapName, valCountMapName,indexKeyCountName,keyHash,keyBA, valBA, hstoreConf.getTimeoutMillis))
    }
    else
      Unit
  }


  def removeEntry(key: K, value: V){
    if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA) = initialVals(key, Option(value))
      execute(new RemoveEntryMono(keyMapName, kvmmName, firstValMapName,valCountMapName,indexKeyCountName,keyHash,keyBA, valBA, hstoreConf.getTimeoutMillis))
    }
  else
      Unit
  }

  def nullcheck[T](t:T, f:T => Unit){if (t != null) f(t) else Unit}

  def removeAllEntries(key: K) {
    if(key !=null)
    {
      val keyHash = hashBaTo5Int(toBA[K](key))
      execute(new RemoveAllEntriesMono(keyMapName, kvmmName, firstValMapName,valCountMapName,indexKeyCountName,keyHash,hstoreConf.getTimeoutMillis))
    }
    else
      Unit
  }

  def findFirst(key: K): V =   {
     val keyHash = hashBaTo5Int(toBA[K](key))
     val first = firstValMap.get(keyHash)
     if (first != null)
       baToT[V](first.data)
      else
       null.asInstanceOf[V]
  }

  def find(key: K): HGRandomAccessResult[V] = {
    val keyHash = hashBaTo5Int(toBA[K](key))
    val valBAWs = kvmm.get(keyHash)
      if (valBAWs == null || valBAWs.isEmpty)
        EmptySR.asInstanceOf[HGRandomAccessResult[V]]
      else
        new HazelRS2[V](valBAWs.toIndexedSeq.map(_.data).sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0})
    }


  def findBase(com: PredicateBuilder, reverse:Boolean) : HGSearchResult[V] =
  {
    val  keySet = keyMap.keySet(com.asInstanceOf[Predicate[FiveInt, ComparableBAW]])
    if  (keySet == null || keySet.size == 0)
      EmptySR.asInstanceOf[HGSearchResult[V]]
    else
    {
      val groupByMember = keySet.groupBy(keyHash => h.getPartitionService.getPartition(keyHash).getOwner)
      val tasks         = groupByMember.map{ case (member, keyHashSet) => new DistributedTask(new GetMultiMappingsFromThatMemberMono(keyMapName,kvmmName,keyHashSet, hstoreConf.getTimeoutMillis),member)}

      tasks.foreach(it => executor.execute(it))

      val tempResult    = tasks.flatMap( future => future.get(2*hstoreConf.getTimeoutMillis, TimeUnit.MILLISECONDS)).toIndexedSeq
      val sortedResult  = tempResult.sortWith{case (k1,k2) => comparator.compare(k1._1.data, k2._1.data)<0}
      new HazelRS2[V](sortedResult.map(_._2).flatten.map(_.data))
    }
  }

  def findLT(key: K)  = findBase(eo.get("this").lessThan(pack(key)),true)
  def findGT(key: K)  = findBase(eo.get("this").greaterThan(pack(key)), false)
  def findLTE(key: K) = findBase(eo.get("this").lessEqual(pack(key)), true)
  def findGTE(key: K) = findBase(eo.get("this").greaterEqual(pack(key)), false)


  def scanKeys(): HGRandomAccessResult[K] = {
    val keyvalues = keyMap.values().map(_.data).toIndexedSeq
    val sorted    = keyvalues.sortWith(comparator.compare(_,_) < 0)
    new HazelRS2[K](sorted)
  }


  def scanValues(): HGRandomAccessResult[V] = {
    val baws = kvmm.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0}
    new HazelRS2[V](baws)
  }

  def count(): Long = indexKeyCount.get
  def count(key: K): Long = valCountMap.get(hashBaTo5Int(toBA[K](key)))

  def open() { }
  def close() {}
  def isOpen: Boolean = (keyMap !=null && kvmm != null)

  def initialVals(key: K,value: Option[V]):(ComparableBAW,FiveInt,BAW) =
  {
    val keyBA = toBA(key)
    val keyHash = hashBaTo5Int(keyBA)
    val valBA = if (value.isDefined) toBA[V](value.get) else Array.empty[Byte]
    val result = (new ComparableBAW(keyBA, comparator), keyHash, BAW(valBA))
    result
  }
}