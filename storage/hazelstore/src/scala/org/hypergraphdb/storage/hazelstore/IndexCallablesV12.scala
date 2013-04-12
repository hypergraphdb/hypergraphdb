package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{Hazelcast, PartitionAware}
import java.util
import com.hazelcast.partition.{MigrationEvent, MigrationListener}

object IndexCallablesV12 {


   class AddEntryMono(keyMapName:String, kvmmName:String,valCountMapName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def getPartitionKey = keyHash
     def call() = {
         val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
//       val config = hi.getConfig
//       config.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))


       val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val localKeyCount    = hi.getAtomicNumber(keyCountName)


       val lock = hi.getLock(keyHash)
       try
       {
         localKeyMap.put(keyHash, keyBA)
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
       finally{
        lock.unlock()
       }
     }
   }

  class AddEntryMono2(keyMapName:String, kvmmName:String, valCountMapName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def call() = {
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
        val a = hi.getConfig.getProperties.get("keyMap").asInstanceOf[ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]]]
        if (a == null)
        {
          val b = new ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]] // (Comparator)
          hi.getConfig.getProperties.put("keyMap", b)
          b
        }
        else
          a
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

      val migrationListener = new MigrationListener {
        def migrationStarted(p1: MigrationEvent) {

        }

        def migrationCompleted(p1: MigrationEvent) {}

        def migrationFailed(p1: MigrationEvent) {}
      }
      Hazelcast.getPartitionService.addMigrationListener(migrationListener)


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



  class RemoveEntryMono(keyMapName:String, kvmmName:String, valCountMapName:String,indexKeyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
//      val hi               = Hazelcast.getDefaultInstance
//val hi               = Hazelcast.newLiteMemberHazelcastInstance()
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next

      val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)


      val lock = hi.getLock(keyHash)
      try
      {
      val kvmmRemoved               = kvmm.remove(keyHash, valBA)
      val valCountOld           = valCountMap.get(keyHash)

      if(kvmmRemoved)
      {
        if (valCountOld == null || valCountOld <= 0)
        // this should never (valCount), rarely (firstVal) happen. But if one of them applies, since we got the valHashes, anyway, we can do them all
        {
          val valBaws = kvmm.get(keyHash)
          valCountMap.put(keyHash,valBaws.size)
        }

        else
        {
          valCountMap.put(keyHash,valCountOld - 1)
          if (valCountOld == 1)
          {
            hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
            try{
              keyMap.lock(keyHash)
              val removed = keyMap.remove(keyHash) != null
              if (!removed)
                println("Index11Callable RemoveEntryMono: remove returned null!")
            } finally {
              keyMap.unlock(keyHash)
            }

          }
        }
      }
      Unit
      }
      finally{
        lock.unlock()
      }
     }

     def getPartitionKey = keyHash
   }




  class RemoveAllEntriesMono(keyMapName:String, kvmmName:String,  valCountMapName:String,keyCountName:String,keyHash:FiveInt,timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def call() {

       val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
       //val config = hi.getConfig
       //config.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val localKeyCount    = hi.getAtomicNumber(keyCountName)

       val lock = hi.getLock(keyHash)
       try
       {

       val removedKvmm    = kvmm.remove(keyHash) != null
       val removedKeyMap  = localKeyMap.remove(keyHash) != null
       valCndMap.put(keyHash,0)//, timeOut, TimeUnit.MILLISECONDS)

       if(removedKvmm != removedKeyMap )
         println("inconsistency in Index during removeAllEntries. removedKvmm should be equal to removedKeyMap")
       if(removedKvmm && removedKeyMap)
        localKeyCount.decrementAndGet()

       Unit
       }
       finally{
         lock.unlock()
       }
     }
     def getPartitionKey = keyHash
   }


  class GetMultiMappingsFromThatMemberMono(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], timeOut:Long) extends Callable[java.util.List[Pair[ComparableBAW,util.Collection[BAW]]]] with Serializable{
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


}
