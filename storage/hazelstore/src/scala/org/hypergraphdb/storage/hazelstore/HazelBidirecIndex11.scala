package org.hypergraphdb.storage.hazelstore

import Callables.BidirCallables11._
import Callables.Index11Callables.GetMultiMappingsFromThatMemberMono
import com.hazelcast.core._
import org.hypergraphdb._
import com.hazelcast.query.{Predicate, PredicateBuilder}
import collection.JavaConversions._


import scala.Serializable
import java.util
import storage.ByteArrayConverter
import storage.hazelstore.Common._
import util.AbstractMap.SimpleEntry
import util.Comparator
import util.concurrent.{TimeUnit, Callable}
import scala.Some
import collection.parallel.ParMap

class HazelBidirecIndex11[K, V] (val name: String,
                                 val h:HazelcastInstance,
                                 hstoreConf:HazelStoreConfig,
                                 implicit val keyConverter:   ByteArrayConverter[K],
                                 implicit val valueConverter: ByteArrayConverter[V],
                                 val providedComparator: Comparator[Array[Byte]] = BAComparator)
  extends HGBidirectionalIndex[K, V] with Serializable {

  // HGBidirectionalIndex adds the requirement to find keys by value
  // The simplest approach would be to extends HazelIndex and add a multimap valToKeysMap 5int-valueHash => 5int-keyHash
  // However, this would imply an overhead on each HGSortIndex operation, because in a operation such as addEntry, kvmm and valToKeysMap would go to different partitions.
  // Since Hazelcast does not provide true distributed transactions (XA)


  // As described in HazelIndex10, hazelcast allows querying for values, but only in Maps not Multimaps.
  // Therefore we cannot querying directly in the multimap mapping single key to many values required for HGSortIndex functions.
  //

  /*--------------------------------------------------------------------------------*/
  type BiKVMM                                     = MultiMap[FiveInt,FiveInt]
  val kvmmName                                    = name + "_kHashToValHashMM"
  val kvmm: BiKVMM                                = h.getMultiMap[FiveInt,FiveInt](kvmmName)
  /*--------------------------------------------------------------------------------*/

  /*--------------------------------------------------------------------------------*/
  val vkmmName                                    = name + "_valHashToKeyHashMM"
  val vkmm: BiKVMM                                = h.getMultiMap[FiveInt,FiveInt](vkmmName)

  /*--------------------------------------------------------------------------------*/

  type ValMap                                     = IMap[FiveInt,BAW]                                                      // mapping combined hash of key + hash of value to Value (BAW). Indexed for quering by value
  val valMapIndexConfig                           = new com.hazelcast.config.MapIndexConfig("data", false)               // importantly, this MapIndexConfig has false, because indexing not required to be >ordered<, since we don't need range queries for findByValue methods.
  val valMapName: String                          = "BidirIndex_" + name + "kvHashToValMap"
  val valMapConfig                                = new com.hazelcast.config.MapConfig(valMapName)
  valMapConfig.addMapIndexConfig(valMapIndexConfig)
  h.getConfig.addMapConfig(valMapConfig)
  val valmap:      ValMap                         = h.getMap[FiveInt,BAW](valMapName)
  /*--------------------------------------------------------------------------------*/
  type KeyMap                                     = IMap[FiveInt, ComparableBAW]
  val mapIndexConfig                              = new com.hazelcast.config.MapIndexConfig("data", true)  // data is the data
  val keyMapName: String                          = name + "keyMap"
  val keyMapConfig                                = new com.hazelcast.config.MapConfig(keyMapName)
  keyMapConfig.addMapIndexConfig(mapIndexConfig)
  h.getConfig.addMapConfig(keyMapConfig)
  val keymap: IMap[FiveInt, ComparableBAW]        = h.getMap[FiveInt, ComparableBAW](keyMapName)

  /*--------------------------------------------------------------------------------*/
  val clusterExecutor = h.getExecutorService
  /*--------------------------------------------------------------------------------*/
  def eo                                          = new PredicateBuilder().getEntryObject
  /*--------------------------------------------------------------------------------*/
  val firstValMapName                             = name + "firstvalMap"
  val firstValMap:IMap[FiveInt,BAW]               = h.getMap[FiveInt,BAW] (firstValMapName)
  /*--------------------------------------------------------------------------------*/
  type ValueCountMap                              = IMap[FiveInt,Long]
  val valueCountMapName                           =   name + "valueCountMap"
  lazy val valCountMap:IMap[FiveInt, Long]        = h.getMap(valueCountMapName)     // if count(key) is never called this doesn't need to be initalized
  /*--------------------------------------------------------------------------------*/
  val indexKeyCountName                           = name + "keyCountOfIndex"
  lazy val indexKeyCount:AtomicNumber             = h.getAtomicNumber(indexKeyCountName)  // if count is never called this doesn't need to be initalized
  /*--------------------------------------------------------------------------------*/
  implicit val comparator:Comparator[Array[Byte]] =
                                 if(providedComparator == null) BAComparator
                                 else providedComparator

  /*--------------------------------------------------------------------------------*/
   val millis = TimeUnit.MILLISECONDS;
  val timeout:Long = hstoreConf.timeoutMillis

  def execute[T](callable: Callable[T], returns:Boolean = false):T =
    if (! callable.isInstanceOf[PartitionAware[FiveInt]])
      callable.call()
    else if (!hstoreConf.getAsync || returns)
      clusterExecutor.submit(callable).get()              // get makes it block == synchronous, but still better to transfer one block of code over to one keyHash owner
    else
      { clusterExecutor.submit(callable); Unit}.asInstanceOf[T]

  def mkHash[T](t:T)(implicit baconverter:ByteArrayConverter[T]):FiveInt = hashBaTo5Int(toBA[T](t)(baconverter))


  def addEntry(key: K, value: V) = if(key !=null && value != null)
  {
    val (keyBA, keyHash, valBA,valHash) = initialVals(key, Option(value))
    execute(new BiAddEntry(keyMapName, kvmmName,vkmmName, valMapName,firstValMapName,valueCountMapName,indexKeyCountName,keyHash,keyBA, valHash, valBA, timeout), false)
  }
  else
    Unit

  def removeEntry(key: K, value: V)   = if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA,valHash) = initialVals(key, Option(value))
      execute(new BiRemoveEntry(keyMapName, kvmmName,vkmmName, valMapName,firstValMapName,valueCountMapName,indexKeyCountName,keyHash,keyBA, valHash, valBA, timeout), false)
    }
  else
    Unit

  def removeAllEntries(key: K)   = if(key !=null )
    {
      val keyHash = hashBaTo5Int(toBA[K](key))
      val valHashs = kvmm.get(keyHash)
      firstValMap.removeAsync(keyHash)
      val groupByMember = valHashs.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
      groupByMember.foreach{ case (member, keysOffThatMember) => clusterExecutor.execute(new DistributedTask(new RemoveAllOnMember(valMapName, vkmmName,keysOffThatMember),member))}    //parallelized
      kvmm.remove(keyHash)
      indexKeyCount.decrementAndGet()
    }
  else
    Unit

  def findFirst(key: K): V =    {
   val keyHash = hashBaTo5Int(toBA[K](key))
   val first = firstValMap.get(keyHash)
   if (first != null)
     baToT[V](first.data)
   else
       null.asInstanceOf[V]
  }

  //ToDo : make a lazy Result Set based on Futures
  def find(key: K): HGRandomAccessResult[V] = {
    val keyHash = mkHash(key)
    val valueHashes = kvmm.get(keyHash)
      if (valueHashes == null || valueHashes.isEmpty)
        EmptySR.asInstanceOf[HGRandomAccessResult[V]]
      else
      {
        val groupByMember = valueHashes.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
        val tasks = groupByMember.map{ case (member, valHashsOnThatMember) => new DistributedTask(new GetItFromThatMember[BAW](valMapName,valHashsOnThatMember),member)}
        tasks.foreach(it => clusterExecutor.execute(it))
        val tempResult = tasks.flatMap( future => future.get(2*timeout, millis).map(baw => baw.data)).toIndexedSeq
        val sortedResult  = tempResult.sortWith{case (k1,k2) => comparator.compare(k1, k2)<0}
        new HazelRS2[V](sortedResult)
      }
    }

  //def groupByMember[C <: Iterable,T](col: C[T]):Map[Member, Iterable[T]] = col.groupBy(a => h.getPartitionService.getPartition(a).getOwner)   // some type prob

  def findXY(com: PredicateBuilder, reverse:Boolean) : HGSearchResult[V] =
  {
    val  keySet = keymap.keySet(com.asInstanceOf[Predicate[FiveInt, ComparableBAW]])
    if  (keySet == null || keySet.size == 0)
      EmptySR.asInstanceOf[HGSearchResult[V]]
    else {
      val groupKeyHashsByMember     = keySet.groupBy(h.getPartitionService.getPartition(_).getOwner)  // groupByMember[util.Set, FiveInt](keySet)
      val keyCbawWithValHashstasks  =
        groupKeyHashsByMember.map
          { case (member, keyHashSet) =>
              {
                val a = new DistributedTask(new GetValHashsForEachKeyHash(keyMapName,kvmmName,keyHashSet, hstoreConf.getTimeoutMillis),member)
                clusterExecutor.execute(a)
                a
              }
          }


      val keyCbawWithValHashs                =    keyCbawWithValHashstasks.flatMap(_.get).toIndexedSeq

      val valHashSet                = new collection.mutable.HashSet[FiveInt]
      keyCbawWithValHashs.foreach(_._2.foreach(valHashSet.+(_)))
      val vals = keyCbawWithValHashs.map(_._2).flatten
      vals

      val groupValHashsByMember = valHashSet.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
      val valBawTasks  =
        groupValHashsByMember
        .map{
            case (member, valHashSet) =>
              {
                val a = new DistributedTask(new GetItFromThatMemberPairedWithHash[BAW](valMapName,valHashSet),member);
                clusterExecutor.execute(a);
                a
              }
            }

      val valHashValBawMap:Map[FiveInt,BAW] = valBawTasks.flatMap(_.get()).toMap

      val combineResults =
        keyCbawWithValHashs
                    .map{case (keyCbaw,valhashs) => (keyCbaw,valhashs.map(valHash => valHashValBawMap(valHash)))}

      val sortedResult = combineResults.sortWith{case (k1,k2) => comparator.compare(k1._1.data, k2._1.data) < 0}.map(_._2.map(baw => baw.data)).flatten
      new HazelRS2[V](sortedResult)
    }
  }

  def findLT(key: K)  = findXY(eo.get("this").lessThan(pack(key)),true)
  def findGT(key: K)  = findXY(eo.get("this").greaterThan(pack(key)), false)
  def findLTE(key: K) = findXY(eo.get("this").lessEqual(pack(key)), true)
  def findGTE(key: K) = findXY(eo.get("this").greaterEqual(pack(key)), false)


  def scanKeys(): HGRandomAccessResult[K] =
    new HazelRS2[K](keymap.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0})


  def scanValues(): HGRandomAccessResult[V] = {
    val result = valmap.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
    new HazelRS2[V](result)
  }

  def open() { }
  def close() {}
  def isOpen: Boolean = (keymap != null && kvmm != null)

  def initialVals(key: K,value: Option[V]) =
  {
    val keyBA = toBA(key)
    val keyHash = hashBaTo5Int(keyBA)
    val valBA = if (value.isDefined) toBA[V](value.get) else Array.empty[Byte]
    val valHash = hashBaTo5Int(valBA)
    val result = (new ComparableBAW(keyBA, comparator), keyHash, BAW(valBA), valHash)
    result
  }

  def count(): Long = indexKeyCount.get
  def count(key: K): Long = valCountMap.get(mkHash[K](key))

  //BiDirectional-Specific Ops
  def countKeys(value:V):Long = execute[Int](new CountKeys(vkmmName,mkHash[V](value)),true).toLong      // callable avoids transfer of entire vkmm.get(valHash) Collection
  def findFirstByValue(value: V): K =
    execute[Option[FiveInt]](new FindFirstByValueKeyHash(vkmmName,keyMapName,mkHash[V](value)), true)   // callable avoids transfer of entire vkmm.get(valHash) Collection
      .flatMap(b => Some(baToT[K](keymap(b).data)))
      .getOrElse(null.asInstanceOf[K])

  def findByValue(value: V): HGRandomAccessResult[K] = {
    val keyHashs = vkmm.get(mkHash[V](value))
    if (keyHashs == null || keyHashs.size == 0)
      EmptySR.asInstanceOf[HGRandomAccessResult[K]]
    else
    {
      val groupByMember = keyHashs.groupBy(keyHash => h.getPartitionService.getPartition(keyHash).getOwner)
      val tasks = groupByMember.map
      {case (member, keysOffThatMember) =>
        {
          val a = new DistributedTask(new GetItFromThatMember[ComparableBAW](keyMapName,keysOffThatMember),member)
          clusterExecutor.execute(a)
          a
        }
      }
      val result = tasks.flatMap(_.get(timeout, millis).map(_.data)).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      new HazelRS2[K](result)
    }
  }
}