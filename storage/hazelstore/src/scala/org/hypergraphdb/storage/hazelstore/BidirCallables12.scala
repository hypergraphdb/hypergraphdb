package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import com.hazelcast.core._
import java.util
import java.util.concurrent.{TimeUnit, Callable}
import scala.Some

object BidirCallables12 {



  class BiAddEntry(keyMapName:String, kvmmName:String,
                   vkmmName:String, valMapName:String,firstValMapName:String,
                   keyCountName:String,valCountMapName:String,
                   keyHash:FiveInt,keyBA:ComparableBAW,
                   valHash:FiveInt, valBA:BAW,
                   useTransactionalInCallables:Boolean, useHCIndexing: Boolean, timeOut:Long,retryCount:Int)
    extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{

    def getPartitionKey = keyHash

    def call() {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()
      val keyMap         = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm           = hi.getMultiMap[FiveInt,FiveInt](kvmmName)
      val vkmm           = hi.getMultiMap[FiveInt,FiveInt](vkmmName)
      val valCountMap    = hi.getMap[FiveInt, Long](valCountMapName)
      val valmap         = hi.getMap[FiveInt, BAW](valMapName)
      val keyCount       = hi.getAtomicNumber(keyCountName)

      println(s"**** bi-add $keyHash ID $ID **** ")

      if (useHCIndexing)
      {
        hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))
        hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(valMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", false)))
      }

      var redo:Int       = retryCount
      var incremKeyCount = false

      while(redo >0)
      {
        val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
        txnOption.map(opt => opt.begin())

        try
        {
          keyMap.put(keyHash, keyBA)
          val kvmmAdded = kvmm.put(keyHash,valHash)
          val vkmmAdded = vkmm.put(valHash,keyHash)

          if(kvmmAdded)
          {
            if(!vkmmAdded) println("bidirIndex.AddEntryOp: kvmmAddded = true but vkmmAdded = false")
            val valCountOld = valCountMap.get(keyHash)
            valCountMap.put(keyHash, valCountOld + 1)

            if (valCountOld == null || valCountOld == 0)
              incremKeyCount = true

            hi.getMap[FiveInt, BAW](firstValMapName).put(keyHash,valBA)
          }

          txnOption.map(_.commit())


          if(incremKeyCount)
            keyCount.incrementAndGet()


          println("now adding to valmap")
          //valmap.putAsync(valHash, valBA)
          var valMapPutSucceeded = false
          while(valMapPutSucceeded == false && (System.nanoTime() - ID) < (3 * timeOut * 1000000)) // timeout are millis, not nanos
          {
            valMapPutSucceeded = valmap.tryPut(valHash, valBA, timeOut, TimeUnit.MILLISECONDS)
          }
          if (!valMapPutSucceeded) println("WARNING: BI-AddEntry: Valmaput failed to succeed ")

          if (redo < retryCount)
            println(s"Bi-AddEntry succeeded after rollback")
          redo = 0

        }
        catch
          { case e:Throwable =>
          {
            e.printStackTrace()
            println("\n\nROLLBACK in Index.addEntry\n\n")
            try    { txnOption.map(_.rollback()) }
            catch  { case e:Throwable => println("\nWarning: Rollback failure!"); e.printStackTrace()  }
            redo = redo -1
            if(redo >0)
              println(s"\n >>> bi-add $keyHash ID $ID  Retrying...")
            else
              println(s"\n !!! W A R N I N G  bi-add $keyHash ID $ID  GIVING UP ...")
          }
          }
      }
    }
  }



  // kept in an imperative java-like style, because these are callables. Dont want to serialize a clumsy scala object-graph with each of them, just for avoiding a while loop
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  class BiRemoveEntry(keyMapName:String,kvmmName:String,
                      vkmmName:String, valMapName:String, firstValMapName:String,
                      indexKeyCountName:String, valCountMapName:String,
                      keyHash:FiveInt,keyBA:ComparableBAW,
                      valHash:FiveInt, valBA:BAW,
                      useTransactionalInCallables:Boolean, timeOut:Long,retryCount:Int)
    extends Callable[Unit] with Serializable with PartitionAware[FiveInt]
  {

    def call()
    {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()

      val keyMap          = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm            = hi.getMultiMap[FiveInt,FiveInt](kvmmName)
      val firstValMap     = hi.getMap[FiveInt, BAW](firstValMapName)
      val valCountMap     = hi.getMap[FiveInt, Long](valCountMapName)
      val vkmm            = hi.getMultiMap[FiveInt,FiveInt](vkmmName)
      println(s"####bi-remove $keyHash ID $ID **** ")

      var redo:Int       = retryCount
      while(redo > 0)
      {
        val txnOption: Option[Transaction] = if (useTransactionalInCallables) Some(hi.getTransaction) else None
        txnOption.map(opt => opt.begin())

        try {
          val kvmmRemoved = kvmm.remove(keyHash, valHash)
          val firstValMapRemoved = firstValMap.remove(keyHash, valBA)
          val valCountOld = valCountMap.get(keyHash)
          vkmm.remove(valHash, keyHash)

          if (kvmmRemoved) {
            if (valCountOld == null || valCountOld <= 0) {
              val valHashs = kvmm.get(keyHash)
              val size = valHashs.size
              valCountMap.put(keyHash, size)
              if (size > 0 && firstValMapRemoved) {
                val valHashIter = valHashs.iterator()
                val valmap = hi.getMap[FiveInt, BAW](valMapName)
                val curVH = valmap.get(valHashIter.next())
                firstValMap.put(keyHash, curVH)
              }
            }

            else {
              valCountMap.put(keyHash, valCountOld - 1)
              if (valCountOld == 1) {
                hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
                keyMap.remove(keyHash)
              }
            }
          }
          txnOption.map(_.commit())
          if (redo < retryCount)
            println(s"Bi-RemoveEntry $keyHash ID $ID  succeeded after rollback")
          redo = 0
        }
        catch {
          case e: Throwable => {
            e.printStackTrace()
            println("\n\nROLLBACK in Index.removeEntry\n\n")
            try {
              txnOption.map(_.rollback())
            }
            catch {
              case e: Throwable => println("\nWarning: Rollback failure!"); e.printStackTrace()
            }
            redo = redo - 1
            if (redo > 0)
              println(s"\n >>> bi-remove $keyHash ID $ID  Retrying...")
            else
              println(s"\n !!! W A R N I N G  bi-remove $keyHash ID $ID  GIVING UP ...")
          }
        }
      }
    }

    def getPartitionKey = keyHash

  }




  class RemoveAllOnMember(valMapName:String, vkmmName:String, valHashs:Iterable[FiveInt],useTransactionalInCallables:Boolean,retryCount:Int) extends Callable[Unit] with Serializable {
    def call() {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()

      //      val keyMap          = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val valMap  = hi.getMap[FiveInt, BAW](valMapName)
      val vkmm    = hi.getMultiMap[FiveInt,FiveInt](vkmmName)
      val it      = valHashs.iterator

      var redo:Int       = retryCount
      while(redo > 0)
      {
        val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
      txnOption.map(opt => opt.begin())

      try
      {
        while (it.hasNext){
          val cur = it.next()
          valMap.remove(cur)
          vkmm.remove(cur)
        }

        txnOption.map(_.commit())
        if (redo < retryCount)
          println(s"Bi-RemoveAllEntry $ID  succeeded after rollback")
        redo = 0
      }
      catch {
        case e: Throwable => {
          e.printStackTrace()
          println("\n\nROLLBACK in Index.removeAll\n\n")
          try {
            txnOption.map(_.rollback())
          }
          catch {
            case e: Throwable => println("\nWarning: Rollback failure!"); e.printStackTrace()
          }
          redo = redo - 1
          if (redo > 0)
            println(s"\n >>> bi-remove ID $ID  Retrying...")
          else
            println(s"\n !!! W A R N I N G  bi-remove ID $ID  GIVING UP ...")
        }
      }
    }
    }
  }


  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
  // BIDIRECTIONAL_INDEX SPECIFIC CALLABLES

  //class GetItFromThatMember[R](mapName:String, keys:Iterable[FiveInt]) extends Callable[util.LinkedList[R]] with Serializable {
  class GetItFromThatMember[R](mapName:String, keys:Iterable[FiveInt]) extends Callable[List[R]] with Serializable {
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()

      val map = hi.getMap[FiveInt,R](mapName)
      println("^^^^ bi-GetItFromThatMember ^^^^")
      val it = keys.iterator
      //val list = new util.LinkedList[R]
      var list = List.empty[R]
      while (it.hasNext){
        val cur = it.next()
        val temp = map.get(cur)
        //if (temp != null) list.add(temp)
        if (temp != null) list = temp :: list
      }
      // println(" end: GetItFromThatMember. List size" + list.size())
      list
    }
  }

  class GetItFromThatMemberPairedWithHash[R](mapName:String, keys:Iterable[FiveInt]) extends Callable[util.LinkedList[Pair[FiveInt, R]]] with Serializable {
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val map = hi.getMap[FiveInt,R](mapName)
      println("bi-GetItFromThatMemberPairedWithHash")
      val it = keys.iterator
      val list = new util.LinkedList[Pair[FiveInt, R]]
      while (it.hasNext){
        val cur = it.next()
        val temp = map.get(cur)
        if (temp != null) list.add(Pair(cur,temp))
      }
      list
    }
  }

  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
  // this is done as callable using Executor service, since this way, it's not necessary to transfer all vkmm.get(valHash) over the wire just to get it's size
  class CountKeys(vkmmName: String, valHash:FiveInt) extends Callable[Int] with PartitionAware[FiveInt] with Serializable {
    def call() ={
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      hi.getMultiMap[FiveInt, FiveInt](vkmmName).get(valHash).size
    }
    def getPartitionKey = valHash
  }
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
  // this is done as callable using Executor service, since this way, it's not necessary to transfer all vkmm.get(valHash) over the wire just to get it's size
  class FindFirstByValueKeyHash(vkmmname: String, keyMapName:String, valHash:FiveInt) extends Callable[Option[FiveInt]] with PartitionAware[FiveInt] with Serializable {
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val vkmm = hi.getMultiMap[FiveInt, FiveInt](vkmmname)
      println("bi-FindFirstByValueKeyHash")
      val iter = vkmm.get(valHash).iterator()
      if (iter.hasNext)
        Some(iter.next)
      else
        None
    }
    def getPartitionKey = valHash
  }
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  class GetValHashsForEachKeyHash(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], timeOut:Long) extends Callable[util.List[Pair[ComparableBAW,util.Collection[FiveInt]]]] with Serializable{
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMultiMap[FiveInt,FiveInt](kvmmName)
      println("bi-GetValHashsForEachKeyHash")
      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[ComparableBAW,util.Collection[FiveInt]]]
      while(keyHashsIterator.hasNext)
      {
        val cur       = keyHashsIterator.next()
        resultList.add(Pair(keyMap.get(cur), kvmm.get(cur)))
      }
      resultList
    }
  }
}
