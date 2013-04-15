package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import com.hazelcast.core.{Transaction, IMap, Hazelcast, PartitionAware}
import java.util
import java.util.concurrent.{TimeUnit, Callable}

object BidirCallables12 {

  // kept in an imperative java-like style, because these are callables. Dont want to serialize a clumsy scala object-graph with each of them, just for avoiding a while loop
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  class BiAddEntry(keyMapName:String, kvmmName:String, vkmmName:String, valMapName:String,firstValMapName:String,valCountMapName:String,keyCountName:String,
                        keyHash:FiveInt,keyBA:ComparableBAW,
                        valHash:FiveInt, valBA:BAW,
                        useTransactionalInCallables:Boolean, useHCIndexing: Boolean, timeOut:Long)
    extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{

     def getPartitionKey = keyHash

     def call() {
       val hi             = Hazelcast.getAllHazelcastInstances.iterator().next

       val keyMap         = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val kvmm           = hi.getMultiMap[FiveInt,FiveInt](kvmmName)
       val vkmm           = hi.getMultiMap[FiveInt,FiveInt](vkmmName)
       val valCountMap    = hi.getMap[FiveInt, Long](valCountMapName)
       val valmap         = hi.getMap[FiveInt, BAW](valMapName)
       val keyCount       = hi.getAtomicNumber(keyCountName)

       if (useHCIndexing)
       {
         hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))
         hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(valMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", false)))
       }

       var redo:Int       = 5
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
             if(!vkmmAdded)
               println("bidirIndex.AddEntryOp: kvmmAddded = true but vkmmAdded = false")
             var valMapPutSucceeded = false
             while(valMapPutSucceeded == false)
             {
               valMapPutSucceeded = valmap.tryPut(valHash, valBA, timeOut, TimeUnit.MILLISECONDS)
             }


             val valCountOld = valCountMap.get(keyHash)
             valCountMap.put(keyHash, valCountOld + 1)

            if (valCountOld == null || valCountOld == 0)
              incremKeyCount = true

             hi.getMap[FiveInt, BAW](firstValMapName).put(keyHash,valBA)
           }
         }
         finally
         {
           try
           {
             txnOption.map(_.commit())
             if(incremKeyCount)
               keyCount.incrementAndGet()
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
                   println("\nRetrying...")
               }
            }
         }
      }
   }
  }

  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  class BiRemoveEntry(keyMapName:String,kvmmName:String, vkmmName:String, valMapName:String, firstValMapName:String,valCountMapName:String,indexKeyCountName:String,
                               keyHash:FiveInt,keyBA:ComparableBAW,
                               valHash:FiveInt, valBA:BAW,
                               useTransactionalInCallables:Boolean, timeOut:Long)
    extends Callable[Unit] with Serializable with PartitionAware[FiveInt]
  {

    def call()
    {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next

      val keyMap          = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm            = hi.getMultiMap[FiveInt,FiveInt](kvmmName)
      val firstValMap     = hi.getMap[FiveInt, BAW](firstValMapName)
      val valCountMap     = hi.getMap[FiveInt, Long](valCountMapName)
      val vkmm            = hi.getMultiMap[FiveInt,FiveInt](vkmmName)

      val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
      txnOption.map(opt => opt.begin())

      try
      {
        val kvmmRemoved         = kvmm.remove(keyHash, valHash)
        val firstValMapRemoved    = firstValMap.remove(keyHash,valBA)
        val valCountOld           = valCountMap.get(keyHash)
        vkmm.remove(valHash, keyHash)

        if(kvmmRemoved)
        {
          if (valCountOld == null || valCountOld <= 0)
          {
            val valHashs = kvmm.get(keyHash)
            val size = valHashs.size
            valCountMap.put(keyHash,size)
            if (size > 0 && firstValMapRemoved) {
              val valHashIter   = valHashs.iterator()
              val valmap = hi.getMap[FiveInt, BAW](valMapName)
              val curVH = valmap.get(valHashIter.next())
              firstValMap.put(keyHash, curVH)
            }
          }

          else
          {
            valCountMap.put(keyHash,valCountOld - 1)
            if (valCountOld == 1)
            {
              hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
              keyMap.remove(keyHash)
            }
          }
        }

        Unit
      }
      finally{
        try  { txnOption.map(_.commit())}
        catch { case e:Throwable => {
          e.printStackTrace()
          println("ROLLBACK in Index.addEntry")
          txnOption.map(_.rollback())
        }
        }
      }
    }

     def getPartitionKey = keyHash

  }




  class RemoveAllOnMember(valMapName:String, vkmmName:String, valHashs:Iterable[FiveInt],useTransactionalInCallables:Boolean) extends Callable[Unit] with Serializable {
    def call() {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      //config.addMapConfig(new com.hazelcast.config.MapConfig(valMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", false)))

      val valMap  = hi.getMap[FiveInt, BAW](valMapName)
      val vkmm    = hi.getMultiMap[FiveInt,FiveInt](vkmmName)
      val it      = valHashs.iterator

      val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
      txnOption.map(opt => opt.begin())

      try
      {
      while (it.hasNext){
        val cur = it.next()
        valMap.remove(cur)
        vkmm.remove(cur)
      }
      Unit
      }
      finally{
        try  { txnOption.map(_.commit())}
        catch { case e:Throwable => {
          e.printStackTrace()
          println("ROLLBACK in Index.addEntry")
          txnOption.map(_.rollback())
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
      val hi = Hazelcast.getAllHazelcastInstances.iterator().next
      val map = hi.getMap[FiveInt,R](mapName)
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
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val map = hi.getMap[FiveInt,R](mapName)
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
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      hi.getMultiMap[FiveInt, FiveInt](vkmmName).get(valHash).size
    }
    def getPartitionKey = valHash
  }
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
  // this is done as callable using Executor service, since this way, it's not necessary to transfer all vkmm.get(valHash) over the wire just to get it's size
  class FindFirstByValueKeyHash(vkmmname: String, keyMapName:String, valHash:FiveInt) extends Callable[Option[FiveInt]] with PartitionAware[FiveInt] with Serializable {
    def call() = {
      val hi = Hazelcast.getAllHazelcastInstances.iterator().next
      val vkmm = hi.getMultiMap[FiveInt, FiveInt](vkmmname)
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
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMultiMap[FiveInt,FiveInt](kvmmName)

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
