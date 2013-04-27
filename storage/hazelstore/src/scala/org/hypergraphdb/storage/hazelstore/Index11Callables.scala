package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.{FiveInt}
import org.hypergraphdb.storage.hazelstore.{BAW, ComparableBAW}
import java.util.concurrent.{ConcurrentSkipListMap, TimeUnit, Callable}
import com.hazelcast.core.{Hazelcast, PartitionAware}
import java.util.AbstractMap.SimpleEntry
import java.util
import java.util.Comparator
import com.hazelcast.partition.{MigrationEvent, MigrationListener}

object Index11Callables {


   class AddEntryMono(keyMapName:String, kvmmName:String, firstValMapName:String,valCountMapName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def getPartitionKey = keyHash
     def call() = {
         val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
//       val config = hi.getConfig
//       config.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

       val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val firstValMap      = hi.getMap[FiveInt, BAW](firstValMapName)
       val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val localKeyCount    = hi.getAtomicNumber(keyCountName)

       localKeyMap.put(keyHash, keyBA)

       val added = kvmm.put(keyHash,valBA)
       if(added)
       {
         val valCountOld = valCndMap.get(keyHash)
         valCndMap.put(keyHash, valCountOld + 1)
         if (valCountOld == null || valCountOld == 0)
           localKeyCount.incrementAndGet()
         firstValMap.put(keyHash,valBA)
       }
       Unit
     }
   }



  class RemoveEntryMono(keyMapName:String, kvmmName:String,firstValMapName:String, valCountMapName:String,indexKeyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
//      val hi               = Hazelcast.getDefaultInstance
//val hi               = Hazelcast.newLiteMemberHazelcastInstance()
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next

      val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val firstValMap         = hi.getMap[FiveInt, BAW](firstValMapName)
      val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)

      val kvmmRemoved               = kvmm.remove(keyHash, valBA)
      val firstValMapRemoved    = firstValMap.remove(keyHash,valBA)
      val valCountOld           = valCountMap.get(keyHash)

      if(kvmmRemoved)
      {
        if (valCountOld == null || valCountOld <= 0)
        // this should never (valCount), rarely (firstVal) happen. But if one of them applies, since we got the valHashes, anyway, we can do them all
        {
          val valBaws = kvmm.get(keyHash)
          valCountMap.put(keyHash,valBaws.size)
          if (firstValMapRemoved) {
            val valBawIter =valBaws.iterator()
            if (valBawIter.hasNext) firstValMap.put(keyHash,valBawIter.next())
          }
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

     def getPartitionKey = keyHash
   }




  class RemoveAllEntriesMono(keyMapName:String, kvmmName:String,firstValMapName:String,  valCountMapName:String,keyCountName:String,keyHash:FiveInt,timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def call() {

       val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
       //val config = hi.getConfig
       //config.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val firstValMap      = hi.getMap[FiveInt, BAW](firstValMapName)
       val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val localKeyCount    = hi.getAtomicNumber(keyCountName)


       val removedKvmm    = kvmm.remove(keyHash) != null
       val removedKeyMap  = localKeyMap.remove(keyHash) != null
       firstValMap.removeAsync(keyHash)
       valCndMap.put(keyHash,0)//, timeOut, TimeUnit.MILLISECONDS)

       if(removedKvmm != removedKeyMap )
         println("inconsistency in Index during removeAllEntries. removedKvmm should be equal to removedKeyMap")
       if(removedKvmm && removedKeyMap)
        localKeyCount.decrementAndGet()
       Unit
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



}
