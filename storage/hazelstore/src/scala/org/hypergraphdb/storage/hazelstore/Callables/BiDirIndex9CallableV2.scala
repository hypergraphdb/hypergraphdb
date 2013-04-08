package org.hypergraphdb.storage.hazelstore.Callables

import org.hypergraphdb.storage.hazelstore.Common.{TenInt, FiveInt}
import org.hypergraphdb.storage.hazelstore.{BAW, ComparableBAW}
import java.util.concurrent.{TimeUnit, Callable}
import com.hazelcast.core.{MapEntry, Hazelcast, PartitionAware}
import com.hazelcast.query.Predicate
import java.util.Comparator

object BiDirIndex9CallableV2 {
/*

   class AddEntryBiIndex9CallableV2(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyCBAW: ComparableBAW, keyvalhash: TenInt, valBAW: BAW, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def getPartitionKey = keyHash
     def call() = {
       val hi               = Hazelcast.getDefaultInstance

       val kvmm             = hi.getMultiMap[FiveInt, TenInt](kvmmName)
       val valmap           = hi.getMap[TenInt, BAW](valMapName)
       val valCountMap      = hi.getMap[FiveInt, Long](valCountMapName)
       val keymap           = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val localKeyCount    = hi.getAtomicNumber(indexKeyCountName)

       keymap.set(keyHash, keyCBAW,timeOut,TimeUnit.MILLISECONDS )
       valmap.set(keyvalhash, valBAW,timeOut,TimeUnit.MILLISECONDS )
       val added = kvmm.put(keyHash,keyvalhash)
       if(added)
       {
         val oldValCount = valCountMap.get(keyHash)
         valCountMap.set(keyHash, oldValCount + 1,timeOut, TimeUnit.MILLISECONDS)

         if (oldValCount == null || oldValCount == 0)
           localKeyCount.incrementAndGet()
       }
       Unit
     }
   }



  class RemoveEntryBiIndex9CallableV2(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyvalhash: TenInt, valBAW: BAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
      val hi               = Hazelcast.getDefaultInstance

      val kvmm             = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap           = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap      = hi.getMap[FiveInt, Long](valCountMapName)
      val keymap           = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val localKeyCount    = hi.getAtomicNumber(indexKeyCountName)

//      val removeVM = valmap.remove(keyvalhash, valBAW)    // the value could be used elsewhere in the indiex!
      kvmm.values.filter( tenInt => )
      val kvmmRemoved = kvmm.remove(keyHash, keyvalhash)

      if (kvmmRemoved != removeVM)
        println(s"Warning: In Bidirindex.removeEntry concerning \n kvMultiMap $kvmmName \nand \nvalMap $valMapName \nput operations did not return equally. Expect inconsistencies! ")

      if (kvmmRemoved) {
        val valCountBefore = valCountMap.get(keyHash)
        if (valCountBefore > 0) valCountMap.put(keyHash, valCountBefore - 1)
        if (valCountBefore == 1) {
          hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
          keymap.remove(keyHash)
        }
        Unit
      }

    }

     def getPartitionKey = keyHash
   }




  class RemoveAllEntriesBiIndex9CallableV2(keyMapName:String, kvmmName:String,firstValMapName:String,  valCountMapName:String,keyCountName:String,keyHash:FiveInt,timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def call() {

       val hi               = Hazelcast.getDefaultInstance

       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val localKeyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val firstValMap         = hi.getMap[FiveInt, BAW](firstValMapName)
       val valCndMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val localKeyCount   = hi.getAtomicNumber(keyCountName)


       val removedKvmm    = kvmm.remove(keyHash) != null
       val removedKeyMap  = localKeyMap.remove(keyHash) != null
       firstValMap.removeAsync(keyHash)
       valCndMap.set(keyHash,0, timeOut, TimeUnit.MILLISECONDS)
       if(removedKvmm != removedKeyMap )
         println("inconsistency in Index during removeAllEntries. removedKvmm should be equal to removedKeyMap")
       if(removedKvmm && removedKeyMap)
        localKeyCount.decrementAndGet()
       Unit
     }
     def getPartitionKey = keyHash
   }


  class UpdateAndReturnBiIndexFindFirst(kvmmName:String,firstValMapName:String, keyHash:FiveInt, timeOut:Long) extends Callable[BAW] with Serializable with PartitionAware[FiveInt]
  {
    def call() = {
      val hi                = Hazelcast.getDefaultInstance
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val firstValMap       = hi.getMap[FiveInt, BAW](firstValMapName)

      val valSet = kvmm.get(keyHash)
      if(valSet == null )
        null.asInstanceOf[BAW]
      else
      {
        val valIt = valSet.iterator()
        if(!valIt.hasNext)
          null.asInstanceOf[BAW]
        else
        {
          val first = valIt.next()
          firstValMap.set(keyHash,first,timeOut,TimeUnit.MILLISECONDS)
          first
        }
      }
    }
    def getPartitionKey = keyHash
  }

  class Find(kvmmName:String,firstValMapName:String, keyHash:FiveInt, timeOut:Long, predicate:Predicate[FiveInt,BAW]) extends Callable[BAW] with Serializable with PartitionAware[FiveInt]
  {
    def call() = {
      val hi                = Hazelcast.getDefaultInstance
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val firstValMap       = hi.getMap[FiveInt, BAW](firstValMapName)

      val predicate = new Predicate[FiveInt,BAW] {
        val comparator : Comparator[Array[Byte]] = ???
        val querySubj :   BAW = ???
        def apply(mapEntry: MapEntry[FiveInt, BAW]) = comparator.compare(mapEntry.getValue.data,querySubj.data) >0
      }
      firstValMap.values(predicate)



    }
    def getPartitionKey = keyHash
  }
  */
}
