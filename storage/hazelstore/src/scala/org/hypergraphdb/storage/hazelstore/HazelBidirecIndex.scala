package org.hypergraphdb.storage.hazelstore

import com.hazelcast.core._
import org.hypergraphdb._
import com.hazelcast.query.{Predicate, PredicateBuilder}
import collection.JavaConversions._


import scala.Serializable
import java.util
import storage.ByteArrayConverter
import storage.hazelstore.Common._
import org.hypergraphdb.storage.hazelstore.testing.TestCommons
import TestCommons._
import java.util.{Collections, Comparator}
import java.util.concurrent.{ConcurrentHashMap, TimeoutException, TimeUnit, Callable}
import org.hypergraphdb.storage.hazelstore.RunnableBackbone.{BiIndexParams, Calloppe, BiIndexStringParams}
import org.hypergraphdb.storage.hazelstore.BidirCallables12._
import scala.Some
import org.hypergraphdb.storage.hazelstore.RunnableBackbone.BiIndexParams
import org.hypergraphdb.storage.hazelstore.RunnableBackbone.BiIndexStringParams


class HazelBidirecIndex[K, V] (val name: String,
                                 val h:HazelcastInstance,
                                 hstoreConf:HazelStoreConfig,
                                 implicit val keyConverter:   ByteArrayConverter[K],
                                 implicit val valueConverter: ByteArrayConverter[V],
                                 val providedComparator: Comparator[Array[Byte]] = BAComparator)
  extends HGBidirectionalIndex[K, V] with Serializable with HGSortIndex[K,V]{

  // HGBidirectionalIndex adds the requirement to find keys by value
  // The simplest approach would be to extends HazelIndex and add a multimap valToKeysMap 5int-valueHash => 5int-keyHash
  // However, this would imply an overhead on each HGSortIndex operation, because in a operation such as addEntry, kvmm and valToKeysMap would go to different partitions.
  // As described in HazelIndex10, hazelcast allows querying for values, but only in Maps not Multimaps.
  // Therefore we cannot querying directly in the multimap mapping single key to many values required for HGSortIndex functions.
  //

  /*--------------------------------------------------------------------------------*/
  type BiKVMM                                     = MultiMap[FiveInt,FiveInt]
  val localKvmmName                                    = name + "_kHashToValHashMM"
  //val localKvmm: BiKVMM                                = h.getMultiMap[FiveInt,FiveInt](localKvmmName)
  val localKvmm:IMap[FiveInt, java.util.Set[FiveInt]] = h.getMap[FiveInt,java.util.Set[FiveInt]](localKvmmName)
  /*--------------------------------------------------------------------------------*/

  /*--------------------------------------------------------------------------------*/
  val localVkmmName                                    = name + "_valHashToKeyHashMM"
  //val localVkmm: BiKVMM                                = h.getMultiMap[FiveInt,FiveInt](localVkmmName)
  val localVkmm: IMap[FiveInt, java.util.Set[FiveInt]] = h.getMap[FiveInt,java.util.Set[FiveInt]](localVkmmName)

  /*--------------------------------------------------------------------------------*/

  type ValMap                                     = IMap[FiveInt,BAW]
  val localValMapName: String                          = "BidirIndex_" + name + "kvHashToValMap"// mapping combined hash of key + hash of value to Value (BAW). Indexed for quering by value
  if(hstoreConf.useHCIndexing)
  {
      val valMapIndexConfig                           = new com.hazelcast.config.MapIndexConfig("data", false)               // importantly, this MapIndexConfig has false, because indexing not required to be >ordered<, since we don't need range queries for findByValue methods.
      val valMapName: String                          = "BidirIndex_" + name + "kvHashToValMap"
      val valMapConfig                                = new com.hazelcast.config.MapConfig(valMapName)
      valMapConfig.addMapIndexConfig(valMapIndexConfig)
      h.getConfig.addMapConfig(valMapConfig)
  }
  val localValmap:      ValMap                         = h.getMap[FiveInt,BAW](localValMapName)

  /*--------------------------------------------------------------------------------*/
  type KeyMap                                     = IMap[FiveInt, ComparableBAW]
  val localKeyMapName: String                          = name + "keyMap"
  if(hstoreConf.useHCIndexing)
  {
      val mapIndexConfig                              = new com.hazelcast.config.MapIndexConfig("data", true)  // data is the data
      val keyMapName: String                          = name + "keyMap"
      val keyMapConfig                                = new com.hazelcast.config.MapConfig(keyMapName)
      keyMapConfig.addMapIndexConfig(mapIndexConfig)
      h.getConfig.addMapConfig(keyMapConfig)
  }
  val localKeymap: IMap[FiveInt, ComparableBAW]        = h.getMap[FiveInt, ComparableBAW](localKeyMapName)

  /*--------------------------------------------------------------------------------*/
  val clusterExecutor = h.getExecutorService
  /*--------------------------------------------------------------------------------*/
  def eo                                          = new PredicateBuilder().getEntryObject
  /*--------------------------------------------------------------------------------*/
  val localFirstValMapName                             = name + "firstvalMap"
  val localFirstValMap:IMap[FiveInt,BAW]               = h.getMap[FiveInt,BAW] (localFirstValMapName)
  /*--------------------------------------------------------------------------------*/
  type ValueCountMap                              = IMap[FiveInt,Long]
  val localValueCountMapName                           =   name + "valueCountMap"
  lazy val localValCountMap:IMap[FiveInt, Long]        = h.getMap(localValueCountMapName)     // if count(key) is never called this doesn't need to be initalized
  /*--------------------------------------------------------------------------------*/
  val localIndexKeyCountName                           = name + "keyCountOfIndex"
  lazy val localIndexKeyCount:AtomicNumber             = h.getAtomicNumber(localIndexKeyCountName)  // if count is never called this doesn't need to be initalized
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
    else if (!hstoreConf.async || returns)
      clusterExecutor.submit(callable).get()              // get makes it block == synchronous, but still better to transfer one block of code over to one keyHash owner
    else
      { clusterExecutor.submit(callable); Unit}.asInstanceOf[T]

  def mkHash[T](t:T)(implicit baconverter:ByteArrayConverter[T]):FiveInt = hashBaTo5Int(toBA[T](t)(baconverter))


  //case class BiIndexStringParams  (kvmmBiName:String, vkmmName:String, valMapName:String,firstValMapName:String,valHash:FiveInt)
  def calloppeShortener(operationName: String, keyHash:FiveInt,keyCBA: Option[ComparableBAW],valHash:FiveInt, valBAW: Option[BAW],
                        fun: (IMap[FiveInt, ComparableBAW], FiveInt, String, IMap[FiveInt,Long],Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams], (String, String,FiveInt,Long)) => (Boolean,String),
                        postfun: (HazelcastInstance,String) => Unit) =
    new Calloppe((name, operationName), localKeyMapName, keyHash, localIndexKeyCountName, keyCBA,valBAW, localValueCountMapName, Right(BiIndexStringParams(localKvmmName,localVkmmName,localValMapName,localFirstValMapName,valHash)), fun, postfun,hstoreConf.transactionalRetryCount, hstoreConf.useTransactionalCallables)


  def addEntry(key: K, value: V){
    if(key !=null && value != null)
      {
        val (keyBA, keyHash, valBA,valHash) = initialVals(key, Option(value))
        val operationName = s"BidirIndex $name addEntry"
        val runnable = calloppeShortener(operationName, keyHash, Some(keyBA), valHash, Some(valBA),
          (funKeyMap: IMap[FiveInt, ComparableBAW],funkeyHash:FiveInt, funKeyCountName:String, valCountMap:IMap[FiveInt,Long],funParams: Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams],id:(String, String, FiveInt,Long)) => {
          funKeyMap.put(keyHash, keyBA)
          funParams.right.get.valMap.put(valHash,valBA)
            val kvmm = funParams.right.get.kvmmBi
            val setKVMM = Option(kvmm.get(keyHash)).getOrElse(Collections.newSetFromMap[FiveInt](new ConcurrentHashMap[FiveInt,java.lang.Boolean](60, 0.8f, 2)))
            val kvmmAdded = setKVMM.add(valHash)
            kvmm.put(keyHash,setKVMM)

            val vkmm = funParams.right.get.vkmm
            val setVKMM = Option(vkmm.get(valHash)).getOrElse(Collections.newSetFromMap[FiveInt](new ConcurrentHashMap[FiveInt,java.lang.Boolean](60, 0.8f, 2)))
            val vkmmAdded = setVKMM.add(keyHash)
            vkmm.put(valHash,setVKMM)
          funParams.right.get.firstValMap.put(keyHash,valBA)

          if(kvmmAdded)
          {
            if(!vkmmAdded) log(s"Warning for $id: kvmmAddded = true but vkmmAdded = false")
            val valCountOld = valCountMap.get(keyHash)
            val newValCount = if(valCountOld == null) 1 else valCountOld +1
            valCountMap.put(keyHash, newValCount)

            if (valCountOld == null || valCountOld == 0)
              (true,funKeyCountName)
            else
              (false,funKeyCountName)
          }
          else
            (false,funKeyCountName)
        },(hi:HazelcastInstance, indexKeyCountName:String) => hi.getAtomicNumber(indexKeyCountName).incrementAndGet())
        execute(runnable)
      }
    else
      Unit
  }


  def removeEntry(key: K, value: V){
    if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA,valHash) = initialVals(key, Option(value))
      val operationName = s"BidirIndex $name removeEntry"
      val runnable = calloppeShortener(operationName, keyHash, Some(keyBA), valHash, Some(valBA),
        (funKeyMap: IMap[FiveInt, ComparableBAW],funkeyHash:FiveInt, funKeyCountName:String, valCountMap:IMap[FiveInt,Long],funParams: Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams],id:(String, String, FiveInt,Long)) => {

          val kvmmBi = funParams.right.get.kvmmBi
          val kvmmSet = kvmmBi.get(keyHash)
          val kvmmRemoved         = kvmmSet.remove(valHash)
          kvmmBi.put(keyHash,kvmmSet)

          val firstValMapRemoved  = funParams.right.get.firstValMap.remove(keyHash, valBA)
          val valCountOld         = valCountMap.get(keyHash)
          funParams.right.get.vkmm.remove(valHash, keyHash)   // WARNING: this is not local to owner node of keyHash, so rollback might not work. If removeEntry operation fails retryCount times and is given up, then vkmm is inconsistent. Should be put into postfun. However starting from Hazelcast 3, there'll be XA transactions

          var keyCountDecrement = false
          if (kvmmRemoved) {
            if (valCountOld == null || valCountOld <= 1) {
              val valHashs = funParams.right.get.kvmmBi.get(keyHash)
              val size = if(valHashs == null) 0 else valHashs.size
              if (size <= 0) {
                funKeyMap.remove(keyHash)
                valCountMap.remove(keyHash)
                keyCountDecrement = true
              }
              else valCountMap.put(keyHash, size)
              if (firstValMapRemoved && size > 0) {
                val valHashIter = valHashs.iterator()
                if(valHashIter.hasNext)
                {
                  val curVH = funParams.right.get.valMap.get(valHashIter.next())
                  funParams.right.get.firstValMap.put(keyHash, curVH)
                }
              }
            }

            else     // valCountOld >1
              valCountMap.put(keyHash, valCountOld - 1)

            (keyCountDecrement,funKeyCountName)
          }
          else
            (false,funKeyCountName)

        },(hi:HazelcastInstance, indexKeyCountName:String) => hi.getAtomicNumber(indexKeyCountName).decrementAndGet())
      execute(runnable)
    }
    else
      Unit

  }

  def removeAllEntries(key: K){
    if(key !=null )
      {
        val keyHash = hashBaTo5Int(toBA[K](key))
        val valHashs = localKvmm.get(keyHash)
        localFirstValMap.removeAsync(keyHash)
        localKeymap.removeAsync(keyHash)
        val groupByMember = valHashs.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
        groupByMember.foreach{ case (member, keysOffThatMember) => clusterExecutor.execute(new DistributedTask(new RemoveAllOnMember(localValMapName, localVkmmName,keysOffThatMember,hstoreConf.useTransactionalCallables,hstoreConf.transactionalRetryCount),member))}    //parallelized
        localKvmm.remove(keyHash)
        localIndexKeyCount.decrementAndGet()
      }
    else
      Unit
  }

  def findFirst(key: K): V =    {
   val keyHash = hashBaTo5Int(toBA[K](key))
   val first = localFirstValMap.get(keyHash)
   if (first != null)
     baToT[V](first.data)
   else
       null.asInstanceOf[V]
  }

  //ToDo : make a lazy Result Set based on Futures
  def find(key: K): HGRandomAccessResult[V] = {
    val keyHash = mkHash(key)
    val valueHashes = localKvmm.get(keyHash)
      if (valueHashes == null || valueHashes.isEmpty)
        EmptySR.asInstanceOf[HGRandomAccessResult[V]]
      else
      {
        val groupByMember = valueHashes.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
        val tasks = groupByMember.map{ case (member, valHashsOnThatMember) => new DistributedTask(new GetItFromThatMember[BAW](localValMapName,valHashsOnThatMember),member)}
        tasks.foreach(it => clusterExecutor.execute(it))
        val tempResult = tasks.flatMap( future => future.get(2*timeout, millis).map(baw => baw.data)).toIndexedSeq
        val sortedResult  = tempResult.sortWith{case (k1,k2) => comparator.compare(k1, k2)<0}
        new HazelRS3[V](sortedResult)
      }
    }

  //def groupByMember[C <: Iterable,T](col: C[T]):Map[Member, Iterable[T]] = col.groupBy(a => h.getPartitionService.getPartition(a).getOwner)   // some type prob

  def findXY(com: PredicateBuilder, reverse:Boolean) : HGSearchResult[V] =
  {
    val  keySet = localKeymap.keySet(com.asInstanceOf[Predicate[FiveInt, ComparableBAW]])
    if  (keySet == null || keySet.size == 0)
      EmptySR.asInstanceOf[HGSearchResult[V]]
    else {
      val groupKeyHashsByMember     = keySet.groupBy(h.getPartitionService.getPartition(_).getOwner)  // groupByMember[util.Set, FiveInt](keySet)
      val keyCbawWithValHashstasks  =
        groupKeyHashsByMember.map
          { case (member, keyHashSet) =>
              {
                val a = new DistributedTask(new GetValHashsForEachKeyHash(localKeyMapName,localKvmmName,keyHashSet, hstoreConf.timeoutMillis),member)
                clusterExecutor.execute(a)
                a
              }
          }


      val keyCbawWithValHashs                =    keyCbawWithValHashstasks.flatMap(_.get).toIndexedSeq
      val vals = keyCbawWithValHashs.map(_._2).flatten

      val groupValHashsByMember = vals.groupBy(valHash => h.getPartitionService.getPartition(valHash).getOwner)
      val valBawTasks  =
        groupValHashsByMember
        .map{
            case (member, valSet) =>
              {
                val a = new DistributedTask(new GetItFromThatMemberPairedWithHash[BAW](localValMapName,valSet),member);
                clusterExecutor.execute(a);
                a
              }
            }

      val valHashValBawMap:Map[FiveInt,BAW] = valBawTasks.flatMap(_.get()).toMap

      val combineResults =
        keyCbawWithValHashs
                    .map{case (keyCbaw,valhashs) => (keyCbaw,valhashs.map(valHash => valHashValBawMap(valHash)))}

      val sortedResult = combineResults.sortWith{case (k1,k2) => comparator.compare(k1._1.data, k2._1.data) < 0}.map(_._2.map(baw => baw.data)).flatten
      new HazelRS3[V](if (reverse) sortedResult.reverse else sortedResult)
    }
  }

  def findLT(key: K)  = findXY(eo.get("this").lessThan(pack(key)),true)
  def findGT(key: K)  = findXY(eo.get("this").greaterThan(pack(key)), false)
  def findLTE(key: K) = findXY(eo.get("this").lessEqual(pack(key)), true)
  def findGTE(key: K) = findXY(eo.get("this").greaterEqual(pack(key)), false)


  def scanKeys(): HGRandomAccessResult[K] =
    HazelRS3[K](localKeymap.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0})


  def scanValues(): HGRandomAccessResult[V] =
    HazelRS3[V](localValmap.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0})

  def open() { }
  def close() {}
  def isOpen: Boolean = (localKeymap != null && localKvmm != null)

  def initialVals(key: K,value: Option[V]) =
  {
    val keyBA = toBA(key)
    val keyHash = hashBaTo5Int(keyBA)
    val valBA = if (value.isDefined) toBA[V](value.get) else Array.empty[Byte]
    val valHash = hashBaTo5Int(valBA)
    val result = (new ComparableBAW(keyBA, comparator), keyHash, BAW(valBA), valHash)
    result
  }

  def count(): Long = localIndexKeyCount.get
  def count(key: K): Long = localValCountMap.get(mkHash[K](key))

  //BiDirectional-Specific Ops
  def countKeys(value:V):Long = execute[Int](new CountKeys(localVkmmName,mkHash[V](value)),true).toLong      // callable avoids transfer of entire vkmm.get(valHash) Collection

  def findFirstByValue(value: V): K =
    execute[Option[FiveInt]](new FindFirstByValueKeyHash(localVkmmName,localKeyMapName,mkHash[V](value)), true)   // callable avoids transfer of entire vkmm.get(valHash) Collection
      //.flatMap(b => Some(baToT[K](keymap(b).data)))
      .map( b => baToT[K](localKeymap(b).data))
      .getOrElse(null.asInstanceOf[K])

  def findByValue(value: V): HGRandomAccessResult[K] = {
    val keyHashs = localVkmm.get(mkHash[V](value))
    if (keyHashs == null || keyHashs.size == 0)
      EmptySR.asInstanceOf[HGRandomAccessResult[K]]
    else
    {
      val groupByMember = keyHashs.groupBy(keyHash => h.getPartitionService.getPartition(keyHash).getOwner)
      val tasks = groupByMember.map
      {case (member, keysOfThatMember) =>
        {
          val a = new DistributedTask(new GetItFromThatMember[ComparableBAW](localKeyMapName,keysOfThatMember),member)
          clusterExecutor.execute(a)
           a
          //val b:Future[java.util.List[ComparableBAW]] = clusterExecutor.submit(fun).asInstanceOf[Future[java.util.List[ComparableBAW]]]
          //val b:Future[List[ComparableBAW]] = clusterExecutor.submit(fun).asInstanceOf[Future[List[ComparableBAW]]]
          //b
        }
      }
      //val result = tasks.flatMap(_.get(timeout, millis).map(_.data)).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      //val result = tasks.flatMap(_.get.map(_.data)).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      //val result = tasks.flatMap(i => {try{Option(i.get)} catch{ case e:TimeoutException => None}}).flatten.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      //val result = tasks.par.flatMap(i => {try{Option(i.get(timeout, millis))} catch{ case e:TimeoutException => None}}).flatten.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      //val result = tasks.par.flatMap(i => {try{Option(i.get)} catch{ case e:TimeoutException => None}}).flatten.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}
      val result = tasks.par.flatMap(i => {try{Option(i.get)} catch{ case e:TimeoutException => None}}).flatten.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2)< 0}

      if (result == null || result.size == 0)
        EmptySR.asInstanceOf[HGRandomAccessResult[K]]
      else
        new HazelRS3[K](result)
    }
  }
}