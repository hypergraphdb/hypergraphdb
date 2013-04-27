package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{TimeUnit, ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{Transaction, Hazelcast, PartitionAware}
import java.util
import com.hazelcast.partition.{MigrationListener, MigrationEvent}

object IndexCallablesV12 {

  class AddEntryMono(keyMapName:String, kvmmName:String,valCountMapName:String,keyCountName:String,
                     keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                     transactional:Boolean, useHCIndexing: Boolean, retryCount:Int) extends Runnable with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def run(){
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()


      if(useHCIndexing)
        hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

      val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)
      val keyCount    = hi.getAtomicNumber(keyCountName)

      var incrementKeyCount = false
      var redo = retryCount
      while(redo >0)
      {
        val txnOpt:Option[Transaction] = if(transactional) Some(hi.getTransaction) else None
        txnOpt.map(_.begin())
        try
        {
          keyMap.put(keyHash, keyBA)
          val added = kvmm.put(keyHash,valBA)
          if(added)
          {
            val valContainedPrev = valCountMap.containsKey(keyHash)
            val valCountOld = valCountMap.get(keyHash)
            val valCountNew = valCountOld + 1
            valCountMap.put(keyHash, valCountNew)
            if (valCountOld == null || valCountOld == 0 || !valContainedPrev){
              incrementKeyCount = true
            }
          }
          txnOpt.map(_.commit())
          if(incrementKeyCount)  keyCount.incrementAndGet()

          if (redo < retryCount)
            println(s"####mono addEntry SUCCEEDED after failure $keyHash id: $ID ####")

          redo = 0
        }
        catch { case e:Throwable => {
          e.printStackTrace()
          println("\n\nROLLBACK in Index.addEntry\n\n")
          try {txnOpt.map(_.rollback())} catch{case e:Throwable => println("\nWarning: Rollback failure!"); e.printStackTrace()}
          redo = redo -1
          if(redo >0)
            println(s"\nRetrying...  $keyHash id: $ID ")
          else
            println(s"\nW A R N I N G   GIVING UP on bi-addEntry  $keyHash  id: $ID ")


        }
        }
      }
    }

  }


  class RemoveEntryMono(keyMapName:String, kvmmName:String, valCountMapName:String,indexKeyCountName:String,
                        keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                        useTransactionalInCallables:Boolean, retryCount:Int) extends Runnable with Serializable with PartitionAware[FiveInt]{
    def run() {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()
      println(s"####mono-remove $keyHash id: $ID ####")

      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyMap            = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val keyCount = hi.getAtomicNumber(indexKeyCountName)

      var keyCountDecrease = false

      var redo = retryCount
      while(redo >0)
      {
        val txnOpt:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
        txnOpt.map(opt => opt.begin())
        try
        {
          val kvmmRemoved    = kvmm.remove(keyHash, valBA)

          if(kvmmRemoved)
          {
            val valCountMap     = hi.getMap[FiveInt, Long](valCountMapName)
            val valCountOld     = valCountMap.get(keyHash)
            if (valCountOld > 0)
            {
              valCountMap.put(keyHash,valCountOld - 1)
              if (valCountOld == 1)
              {
                keyCountDecrease = true
                val removed = keyMap.remove(keyHash) != null
                if (!removed)
                  println(s"!!!!!!mono-remove $keyHash id: $ID #### Index11Callable RemoveEntryMono possible inconsistency: keyMap remove returned null where it should not.")
              }
            }
          }

          txnOpt.map(_.commit())
          if (keyCountDecrease) {
            //println("Index.removeEntry: keyCount is now decremented. ")
            keyCount.decrementAndGet()

          }

          if (redo < retryCount)
            println(s"####mono removeEntry SUCCEEDED after failure $keyHash id: $ID ####")

          redo = 0
        }
        catch { case e:Throwable => {
          e.printStackTrace()
          println(s"\n\n\n!!!!!!ROLLBACK in Index.removeEntry $keyHash id: $ID \n\n")
          try {txnOpt.map(_.rollback())} catch{case e:Throwable => println(s"\nWarning: Rollback failure! $keyHash id: $ID ####"); e.printStackTrace()}
          redo = redo -1
          if(redo >0)
            println(s"\nRetrying Index.removeEntry $keyHash id: $ID ")
          else
            println(s"\nW A R N I N G   GIVING UP on removeEntry $keyHash id: $ID ")
        }
        }
      }
    }

    def getPartitionKey = keyHash
  }




  class RemoveAllEntriesMono(keyMapName:String, kvmmName:String,  valCountMapName:String,keyCountName:String,
                             keyHash:FiveInt,
                             useTransactionalInCallables:Boolean, retryCount:Int) extends Runnable with Serializable with PartitionAware[FiveInt]{
    def run() {

      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()
      println("~~~~ mono-removeAll $keyHash id: $ID ~~~~")

      val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)
      val keyCount    = hi.getAtomicNumber(keyCountName)


      var redo = retryCount
      while(redo >0)
      {
        val txnOpt:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
        txnOpt.map(opt => opt.begin())

        var keyCoundDecrement:Boolean = false
        try
        {
          val a              = kvmm.remove(keyHash)
          val removedKvmm    = a != null && a.size != 0
          val removedKeyMap  = keyMap.remove(keyHash) != null
          valCountMap.put(keyHash,0)

          if(removedKvmm != removedKeyMap )
            println("inconsistency in Index " +  keyMapName.replace("keyMap","") + " during removeAllEntries. removedKvmm should be equal to removedKeyMap")
          if(removedKvmm && removedKeyMap)
            keyCoundDecrement = true

          txnOpt.map(_.commit())
          if (keyCoundDecrement)   keyCount.decrementAndGet()
          Unit

          if (redo < retryCount)
            println(s"####mono removeAllEntry SUCCEEDED after failure $keyHash id: $ID ####")

          redo = 0
        }
        catch { case e:Throwable => {
          e.printStackTrace()
          println(s"\n\nROLLBACK in Index.removeAll $keyHash id: $ID \n\n")
          try {txnOpt.map(_.rollback())} catch{case e:Throwable => println(s"\n\nWarning: Rollback failure! $keyHash id: $ID "); e.printStackTrace()}
          redo = redo -1
          if(redo >0)
            println("\nRetrying...")
          else
            println(s"\nW A R N I N G   GIVING UP on removeEntry $keyHash id: $ID ")

        }
        }
      }
    }
    def getPartitionKey = keyHash
  }


  class GetMultiMappingsFromThatMemberMono(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], retryCount:Int) extends Callable[java.util.List[Pair[ComparableBAW,util.Collection[BAW]]]] with Serializable{
    def call() = {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[ComparableBAW,util.Collection[BAW]]]
      while(keyHashsIterator.hasNext)
      {
        val cur = keyHashsIterator.next()
        resultList.add(Pair(keyMap.get(cur), kvmm.get(cur)))
      }
      resultList
    }
  }


  class FindFirstOp(kvmmName:String, keyHash:FiveInt, timeOut:Long) extends Callable[BAW] with Serializable with PartitionAware[FiveInt]{
    def call() = {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)

      val valHashIt = kvmm.get(keyHash).iterator()
      if(valHashIt.hasNext)
        valHashIt.next()
      else
        null.asInstanceOf[BAW]
    }

    def getPartitionKey = keyHash
  }


  /*
  // experimental
  class FindBase(keyMapName:String, valMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], queryKey:BAW, comparator:Comparator[Array[Byte]], greater:Boolean, equal:Boolean, timeOut:Long)
    extends Callable[java.util.List[Pair[BAW,util.Collection[BAW]]]] with Serializable{
    def call() = {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val keyMap            = hi.getMap[FiveInt,BAW](keyMapName)
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val valMap            = hi.getMap[FiveInt, BAW](valMapName)



      // keyMap.localKeySet()

      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[BAW,util.Collection[BAW]]]
      while(keyHashsIterator.hasNext)
      {
        val cur = keyHashsIterator.next()
        val curKey = keyMap.get(cur)
        val x = comparator.compare(curKey.data,queryKey.data)
        if((x == 0 && equal) || (x == -1 && !greater) || (x == 1 && greater))
        {
          val curValsHashs = kvmm.get(cur).iterator()
          val vals = new util.LinkedList[BAW]
          while( curValsHashs.hasNext)
          {
            val curVal = curValsHashs.next
            vals.add(valMap.get(curValsHashs.next))
          }
          resultList.add(new Pair(curKey, vals))
        }
      }
      resultList
    }
  } */


  //Experimental
  /*

    class AddEntryMono2(keyMapName:String, kvmmName:String, valCountMapName:String,keyCountName:String,
                        keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                        timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
      def getPartitionKey = keyHash
      def call() {
        val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
        //       val config = hi.getConfig
        //       config.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

        val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
        val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
        val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
        val localKeyCount   = hi.getAtomicNumber(keyCountName)

        localKeyMap.put(keyHash, keyBA)

        val nvm =
        {
          val fun = hi.getConfig.getProperties.get("keyMap").asInstanceOf[ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]]]
          if (fun == null)
          {
            val b = new ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]] // (Comparator)
            hi.getConfig.getProperties.put("keyMap", b)
            b
          }
          else
            fun
        }

        val list = nvm.get(keyBA)
        val newList:java.util.List[BAW] =
          if(list == null || list.isEmpty)
          {
            val createdList = new util.LinkedList[BAW]()
            createdList.add(valBA)
            createdList
          }
          else {
            list.add(valBA)
            list
          }
        nvm.put(keyBA,newList)

        val partitionService = hi.getPartitionService

        val migrationListener = new MigrationListener {
          def migrationStarted(p1: MigrationEvent) {
            val partitionId = p1.getPartitionId
            val iterator = nvm.entrySet().iterator()
            iterator.foreach( mapping => if (partitionService.getPartition(mapping.getKey).getPartitionId == partitionId) iterator.remove())
            continue dis



          }

          def migrationCompleted(p1: MigrationEvent) {}

          def migrationFailed(p1: MigrationEvent) {}
        }

        partitionService.addMigrationListener(migrationListener)


        val added = kvmm.put(keyHash,valBA)
        if(added)
        {
          val valCountOld = valCndMap.get(keyHash)
          valCndMap.put(keyHash, valCountOld + 1)
          if (valCountOld == null || valCountOld == 0)
            localKeyCount.incrementAndGet()
        }
        Unit
      }
    }
    */

}
