package org.hypergraphdb.storage.hazelstore

import com.hazelcast.core._
import org.hypergraphdb._
import com.hazelcast.query.{Predicate, PredicateBuilder}
import collection.JavaConversions._
import scala.Serializable
import java.util
import storage.ByteArrayConverter
import storage.hazelstore.Common._
import java.util.{Collections, Comparator}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, Callable}

import org.hypergraphdb.storage.hazelstore.RunnableBackbone.{Calloppe, BiIndexParams}
import org.hypergraphdb.storage.hazelstore.IndexCallablesV12.{FindFirstOp, GetMultiMappingsFromThatMemberMono}


class HazelIndex[K, V] (val name: String,
                          val h:HazelcastInstance,
                          implicit val hstoreConf:HazelStoreConfig,
                          implicit val keyConverter:   ByteArrayConverter[K],
                          implicit val valueConverter: ByteArrayConverter[V],
                          val providedComparator: Comparator[Array[Byte]] = BAComparator)
  extends HGSortIndex[K, V] with Serializable {


  /*--------------------------------------------------------------------------------*/
  val localKvmmName                                    = name + "keyValueMultiMap"
  //val localKvmm:MultiMap[FiveInt, BAW]                                   = h.getMultiMap(localKvmmName)
  val localKvmm:IMap[FiveInt, java.util.Set[BAW]]  = h.getMap(localKvmmName)
  /*--------------------------------------------------------------------------------*/
  val localKeyMapName: String                          = name + "keyMap"
  if(hstoreConf.useHCIndexing){
    val mapIndexConfig                              = new com.hazelcast.config.MapIndexConfig("data", true)
    val keyMapConfig                                = new com.hazelcast.config.MapConfig(localKeyMapName)
    keyMapConfig.addMapIndexConfig(mapIndexConfig)
    h.getConfig.addMapConfig(keyMapConfig)
  }
  val localKeyMap: IMap[FiveInt, ComparableBAW]                              = h.getMap(localKeyMapName)

  /*--------------------------------------------------------------------------------*/

  /*--------------------------------------------------------------------------------*/
  //  type Args       = (ComparableBAW, FiveInt, BAW)
  def eo                                          = new PredicateBuilder().getEntryObject
  /*--------------------------------------------------------------------------------*/
  val localValCountMapName                             = name + "valCountMap"
  val localValCountMap:IMap[FiveInt, Long]             = h.getMap(localValCountMapName)
  /*--------------------------------------------------------------------------------*/
  val localIndexKeyCountName                           = "keyCountOfIndex" + name
  def localIndexKeyCount                               = h.getAtomicNumber(localIndexKeyCountName)

  /*--------------------------------------------------------------------------------*/

  implicit val comparator:Comparator[Array[Byte]] =
    if(providedComparator == null) BAComparator
    else providedComparator

  val executor = h.getExecutorService

  def execute[T](callable: Callable[T], returns:Boolean = false):T =
    if (! callable.isInstanceOf[PartitionAware[FiveInt]])
      callable.call()
    else if (!hstoreConf.async || returns)
      executor.submit(callable).get()              // get makes it block == synchronous, but still better to transfer one block of code over to one keyHash owner
    else
      { executor.submit(callable); Unit}.asInstanceOf[T]

  def execute(runnable: Runnable){
    if (!hstoreConf.async)
      runnable.run
    else
      executor.execute(runnable)
  }

  def calloppeShortener(operationName: String, keyHash:FiveInt,keyCBA: Option[ComparableBAW],valBAW: Option[BAW],
                        fun: (IMap[FiveInt, ComparableBAW], FiveInt, String, IMap[FiveInt,Long],Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams], (String, String,FiveInt,Long)) => (Boolean,String),
               postfun: (HazelcastInstance,String) => Unit) =
    new Calloppe((name, operationName), localKeyMapName, keyHash, localIndexKeyCountName, keyCBA,valBAW, localValCountMapName, Left(localKvmmName), fun, postfun,hstoreConf.transactionalRetryCount, hstoreConf.useTransactionalCallables)

  def addEntry(key: K, value: V) {
    if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA) = initialVals(key, Option(value))
      val callable = calloppeShortener(s"MonoIndex $name -addEntry", keyHash, keyCBA = Some(keyBA),valBAW = Some(valBA),
        (funKeyMap: IMap[FiveInt, ComparableBAW],funkeyHash:FiveInt, funKeyCountName:String, valCountMap:IMap[FiveInt,Long],funParams: Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams],id:(String, String, FiveInt,Long)) =>
        {
          val incrementKeyCount = funKeyMap.put(funkeyHash, keyBA) == null
          val kvmm = funParams.left.get
          val set = Option(kvmm.get(funkeyHash)).getOrElse(Collections.newSetFromMap[BAW](new ConcurrentHashMap[BAW,java.lang.Boolean](60, 0.8f, 2)))
          val incrementValueCount  = set.add(valBA)
          kvmm.put(funkeyHash, set)

          if(incrementValueCount)
          {
            val valCountOld       = valCountMap.get(funkeyHash)
            val valCountNew       = valCountOld + 1
            valCountMap.put(funkeyHash, valCountNew)
          }
          (incrementKeyCount, funKeyCountName)
        },
        (hi:HazelcastInstance, indexKeyCountName:String) => { hi.getAtomicNumber(indexKeyCountName).incrementAndGet()})

      execute(callable)
    }
    else
      Unit
  }


  def removeEntry(key: K, value: V){
    if(key !=null && value != null)
    {
      val (keyBA, keyHash, valBA) = initialVals(key, Option(value))
      val callable = calloppeShortener(s"MonoIndex $name -removeEntry", keyHash, keyCBA = Some(keyBA),valBAW = Some(valBA),
        (funKeyMap: IMap[FiveInt, ComparableBAW],funkeyHash:FiveInt, funKeyCountName:String, valCountMap:IMap[FiveInt,Long],funParams: Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams],id:(String, String, FiveInt,Long)) =>
        {
          val kvmm = funParams.left.get
          val set = Option(kvmm.get(funkeyHash))
          val removed = set.map(_.remove(valBA))
          set.map(kvmm.put(funkeyHash,_))

          if(removed.isDefined && removed.get)
          {
            val valCountOld     = valCountMap.get(keyHash)

            valCountOld match {
              case i if i > 1 =>  {
                      valCountMap.put(keyHash,valCountOld - 1);
                      (false,funKeyCountName)
                    }
              case i if i <= 1 => {
                      funKeyMap.remove(keyHash)
                      valCountMap.remove(keyHash)
                      (true,funKeyCountName)
                    }
            }
          }
          else  (true,funKeyCountName)
        },
          (hi:HazelcastInstance, indexKeyCountName:String) => { hi.getAtomicNumber(indexKeyCountName).decrementAndGet()})

          execute(callable)
        }
      else
      Unit
    }

    def removeAllEntries(key: K) {
      if(key !=null)
      {
        val keyHash = hashBaTo5Int(toBA[K](key))
        val callable = calloppeShortener(s"MonoIndex $name -removeAllEntries", keyHash, keyCBA = None,valBAW = None,
          (funKeyMap: IMap[FiveInt, ComparableBAW],funkeyHash:FiveInt, funKeyCountName:String, valCountMap:IMap[FiveInt,Long],funParams: Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams],id:(String, String, FiveInt,Long)) =>
          {
            val a              = funParams.left.map(_.remove(keyHash))
            //val removedKvmm    = a != null && a.left.get.size != 0
            val removedKeyMap  = funKeyMap.remove(keyHash) != null
            valCountMap.put(keyHash,0)
            if  (removedKeyMap) (true, funKeyCountName)
            else                (false, funKeyCountName)
          },
          (hi:HazelcastInstance, indexKeyCountName:String) => hi.getAtomicNumber(indexKeyCountName).decrementAndGet
        )
        execute(callable)
      }
      else
        Unit
    }

    def findFirst(key: K): V =
      if(key !=null)
      {
        val keyHash = hashBaTo5Int(toBA[K](key))
        val valbaw = execute(new FindFirstOp(localKvmmName, keyHash,hstoreConf.timeoutMillis), true)
        if(valbaw != null)
          baToT[V](valbaw.data)
        else
          null.asInstanceOf[V]
      }
      else
        null.asInstanceOf[V]

    def find(key: K): HGRandomAccessResult[V] = {
      val keyHash = hashBaTo5Int(toBA[K](key))
      val valBAWs = localKvmm.get(keyHash)
      if (valBAWs == null || valBAWs.isEmpty)
        EmptySR.asInstanceOf[HGRandomAccessResult[V]]
      else
        new HazelRS3[V](valBAWs.toIndexedSeq.map(_.data).sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0})
    }


    def findBase(com: PredicateBuilder, reverse:Boolean) : HGSearchResult[V] =
    {
      val  keySet = localKeyMap.keySet(com.asInstanceOf[Predicate[FiveInt, ComparableBAW]])
      if  (keySet == null || keySet.size == 0)
        EmptySR.asInstanceOf[HGSearchResult[V]]
      else
      {
        val groupByMember = keySet.groupBy(keyHash => h.getPartitionService.getPartition(keyHash).getOwner)
        val tasks         = groupByMember.map{ case (member, keyHashSet) => new DistributedTask(new GetMultiMappingsFromThatMemberMono(localKeyMapName,localKvmmName,keyHashSet, hstoreConf.timeoutMillis),member)}

        tasks.foreach(it => executor.execute(it))

        val tempResult    = tasks.flatMap( future => future.get(2*hstoreConf.timeoutMillis, TimeUnit.MILLISECONDS)).toIndexedSeq
        val sortedResult  = tempResult.sortWith{case (k1,k2) => comparator.compare(k1._1.data, k2._1.data)<0}
        val mappedSorted  = (if(reverse) sortedResult.reverse else sortedResult).map(_._2).flatten.map(_.data)
        new HazelRS3[V](mappedSorted)
      }
    }

    def findLT(key: K)  = findBase(eo.get("this").lessThan(pack(key)),true)
    def findGT(key: K)  = findBase(eo.get("this").greaterThan(pack(key)), false)
    def findLTE(key: K) = findBase(eo.get("this").lessEqual(pack(key)), true)
    def findGTE(key: K) = findBase(eo.get("this").greaterEqual(pack(key)), false)


    def scanKeys(): HGRandomAccessResult[K] = {
      val keyvalues = localKeyMap.values().map(_.data).toIndexedSeq
      if(keyvalues == null || keyvalues.size == 0)
        EmptySR.asInstanceOf[HGRandomAccessResult[K]]
      else
      {
        val sorted    = keyvalues.sortWith(comparator.compare(_,_) < 0)
        new HazelRS3[K](sorted)
      }
    }


    def scanValues(): HGRandomAccessResult[V] = {
      //val baws = localKvmm.values.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0}
      val baws = localKvmm.values.flatten.map(_.data).toIndexedSeq.sortWith{case (k1,k2) => comparator.compare(k1,k2) < 0}
      new HazelRS3[V](baws)
    }

    def count(): Long =   { val count = localIndexKeyCount.get
    if (count== null) 0 else count }

    def count(key: K): Long = { val count = localValCountMap.get(hashBaTo5Int(toBA[K](key)))
      if (count== null) 0 else count }

    def open() { }
    def close() {}
    def isOpen: Boolean = (localKeyMap !=null && localKvmm != null)

    def initialVals(key: K,value: Option[V]):(ComparableBAW,FiveInt,BAW) =
    {
      val keyBA = toBA(key)
      val keyHash = hashBaTo5Int(keyBA)
      val valBA = if (value.isDefined) toBA[V](value.get) else Array.empty[Byte]
      val result = (new ComparableBAW(keyBA, comparator), keyHash, BAW(valBA))
      result
    }
  }