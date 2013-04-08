package org.hypergraphdb.storage.hazelstore.Callables

import scala.Serializable
import com.hazelcast.core._
import org.hypergraphdb.storage.hazelstore.Common._
import java.util.concurrent.{Future, Callable}
import org.hypergraphdb.storage.hazelstore.{ComparableBAW, BAW}


object BiDirRunnablesCallables {}
 /*
  class FindFirstBiOp(kvmmName: String, valMapName: String, keyHash: FiveInt) extends Callable[BAW] with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def call() = {
      val hi = Hazelcast.getDefaultInstance
      val keyMap = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valMap = hi.getMap[TenInt, BAW](kvmmName)

      val valHashs    = keyMap.get(keyHash).iterator()
      var result: BAW = BAW(Array.empty[Byte])

      while(valHashs.hasNext && result == null){    // for some reason, valHashs could be out of date, and point to null
        result = valMap.get(valHashs.next())
      }
      result
    }
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  class BiAddEntryOpNoLock(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyCBAW: ComparableBAW, keyvalhash: TenInt, valBAW: BAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val localUUID = hi.getCluster.getLocalMember.getUuid
      val weAreLocal = false //localUUID.equals(originUUID)

      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

        val addedvalmap = valmap.put(keyvalhash, valBAW) == null
        val kvmmAdded = kvmm.put(keyHash, keyvalhash)
        keymap.putIfAbsent(keyHash, keyCBAW)

        if (kvmmAdded != addedvalmap)
          println(s"Warning: In Bidirindex.addEntry concerning \n kvMultiMap $kvmmName \nand \nvalMap $valMapName \nput operations did not return equally. Expect inconsistencies! ")

        if (kvmmAdded) {
          val valCountOld = valCountMap.get(keyHash)
          valCountMap.put(keyHash, valCountOld + 1)
          if (valCountOld == 0)
            hi.getAtomicNumber(indexKeyCountName).incrementAndGet()
        }
      Unit
    }

    def getPartitionKey = keyHash
  }

  class BiAddEntryOp(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyCBAW: ComparableBAW, keyvalhash: TenInt, valBAW: BAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val localUUID = hi.getCluster.getLocalMember.getUuid
      val weAreLocal = false //localUUID.equals(originUUID)

      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

      try {
          valmap.lock(keyvalhash)
          kvmm.lock(keyHash)
          keymap.lock(keyHash)
          valCountMap.lock(keyHash)

        val addedvalmap = valmap.put(keyvalhash, valBAW) == null
        val kvmmAdded = kvmm.put(keyHash, keyvalhash)
        keymap.putIfAbsent(keyHash, keyCBAW)

        if (kvmmAdded != addedvalmap)
          println(s"Warning: In Bidirindex.addEntry concerning \n kvMultiMap $kvmmName \nand \nvalMap $valMapName \nput operations did not return equally. Expect inconsistencies! ")

        if (kvmmAdded) {
          val valCountOld = valCountMap.get(keyHash)
          valCountMap.put(keyHash, valCountOld + 1)
          if (valCountOld == 0)
            hi.getAtomicNumber(indexKeyCountName).incrementAndGet()
        }
      }
      finally {
        valmap.unlock(keyvalhash)
        kvmm.unlock(keyHash)
        keymap.unlock(keyHash)
        valCountMap.unlock(keyHash)
      }
      Unit
    }

    def getPartitionKey = keyHash
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  class BiRemoveEntryOpNoLock(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyvalhash: TenInt, valBAW: BAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

//        val removeVM = valmap.remove(keyvalhash, valBAW)  // the value could be used elsewhere in the indiex!
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


  class BiRemoveEntryOp(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt, keyvalhash: TenInt, valBAW: BAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

      try {
        valmap.lock(keyvalhash)
        kvmm.lock(keyHash)
        keymap.lock(keyHash)
        valCountMap.lock(keyHash)

        val removeVM = valmap.remove(keyvalhash, valBAW)
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
      finally {
        valmap.unlock(keyvalhash)
        kvmm.unlock(keyHash)
        keymap.unlock(keyHash)
        valCountMap.unlock(keyHash)
      }
    }

    def getPartitionKey = keyHash
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  class BiRemoveAllEntriesOpNoLock(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

      val keymapremoved = keymap.remove(keyHash)
      val valuesAssociatedWithKey = kvmm.get(keyHash).iterator()
      while (valuesAssociatedWithKey.hasNext)
      {
        val currentKey = valuesAssociatedWithKey.next()
        valmap.remove(currentKey)
      }
      val removedKVMM = kvmm.remove(keyHash).size() // hmm. instead of boolean, remove actually returns the old collection contained in! That's okish for kvmm since it's only hashes & local to call if non-transactional, but in transactional context, this delays release of locks by at least one entire network round trip.
      if (removedKVMM > 0) {
        valCountMap.put(keyHash, 0)
        hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
      }
      Unit
    }

    def getPartitionKey = keyHash
  }


  class BiRemoveAllEntriesOp(keyMapName: String, valMapName: String, kvmmName: String, valCountMapName: String, indexKeyCountName: String, keyHash: FiveInt) extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def call() {
      val hi = Hazelcast.getDefaultInstance
      val keymap = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val kvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)
      val valmap = hi.getMap[TenInt, BAW](valMapName)
      val valCountMap = hi.getMap[FiveInt, Long](valCountMapName)

      try {
        kvmm.lock(keyHash)
        keymap.lock(keyHash)
        valCountMap.lock(keyHash)

        val keymapremoved = keymap.remove(keyHash)

        val valuesAssociatedWithKey = kvmm.get(keyHash).iterator()
        while (valuesAssociatedWithKey.hasNext) {
          val currentKey = valuesAssociatedWithKey.next()
          try {
            valmap.lock(currentKey)
            valmap.remove(currentKey)
          } finally {
            valmap.unlock(currentKey)
          }
        }

        val removedKVMM = kvmm.remove(keyHash).size() // hmm. instead of boolean, remove actually returns the old collection contained in! That's okish for kvmm since it's only hashes & local to call if non-transactional, but in transactional context, this delays release of locks by at least one entire network round trip.

        if (removedKVMM > 0) {
          valCountMap.put(keyHash, 0)
          hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
        }

        Unit
      }
      finally {
        kvmm.unlock(keyHash)
        keymap.unlock(keyHash)
        valCountMap.unlock(keyHash)
      }
    }

    def getPartitionKey = keyHash
  }



 class BiAddEntryCallable (keymap:IMap[FiveInt,ComparableBAW],valmap:IMap[TenInt,BAW],kvmm:MultiMap[FiveInt,TenInt],valueCountOfKey:AtomicNumber,indexKeyCount:AtomicNumber, keyHash:FiveInt, keyCBAW:ComparableBAW,keyvalhash:TenInt, valBAW:BAW) extends  Callable[Unit] with Serializable with PartitionAware[FiveInt]
 {
   def call() {
     keymap.putIfAbsent(keyHash, keyCBAW)
     val addedvalmap   = valmap.put(keyvalhash, valBAW)
     val addedkvmm     = kvmm.put(keyHash,keyvalhash)
     if ( addedkvmm != (addedvalmap == null)) println(s"Warning: Bidirindex: addEntry: kvMultiMap's and valMap's put operations did not return equally. Expect inconsistencies!") //Index name: $name . key: $key . value: $value ")       // this will possible cause both key and value object to be packed into the Callable
     if(addedkvmm){
       val valCount  = valueCountOfKey.incrementAndGet()
       if (valCount  == 1) indexKeyCount.incrementAndGet()
     }
     Unit
   }
   def getPartitionKey = keyHash
 }

 class BiRemoveEntryCallable (keymap:IMap[FiveInt,ComparableBAW],valmap:IMap[TenInt,BAW],kvmm:MultiMap[FiveInt,TenInt],valueCountOfKey:AtomicNumber,indexKeyCount:AtomicNumber, keyHash:FiveInt, keyvalhash:TenInt, valBAW:BAW) extends  Callable[Unit] with Serializable with PartitionAware[FiveInt]
 {
   def call() {
     val removeVM      = valmap.remove(keyvalhash, valBAW)
     val removedKVMM   = kvmm.remove(keyHash,keyvalhash)
     if (removedKVMM != removeVM)  println(s"Warning: Bidirindex: removeEntry: kvMultiMap's and valMap's remove operations did not return equally. Expect inconsistencies! ")// Index name: $name . key: $key . value: $value ")
     if(removedKVMM){
       val valCount = valueCountOfKey.decrementAndGet()
       if (valCount == 0) {
         indexKeyCount.decrementAndGet()
         keymap.remove(keyHash)
       }
     }
   Unit
   }
   def getPartitionKey = keyHash
 }

 class BiRemoveAllEntriesCallable (keymap:IMap[FiveInt,ComparableBAW],valmap:IMap[TenInt,BAW],kvmm:MultiMap[FiveInt,TenInt],valueCountOfKey:AtomicNumber,indexKeyCount:AtomicNumber, keyHash:FiveInt) extends  Callable[Unit] with Serializable with PartitionAware[FiveInt]
 {
   import collection.JavaConversions._
   def call() {
     keymap.remove(keyHash)
     val valuesAssociatedWithKey = kvmm.get(keyHash)
     valuesAssociatedWithKey.foreach(tenInt => valmap.remove(tenInt))
     val removedKVMM = kvmm.remove(keyHash).size()      // hmm. instead of boolean, remove actually returns the old collection contained in! That's ok for kvmm since it's only hashes.
     if(removedKVMM > 0)
     {
       valueCountOfKey.destroy()
       indexKeyCount.decrementAndGet()
     }
     Unit

   }
   def getPartitionKey = keyHash
 }


 class BiAddEntryRunnable (keyMapName:String, valMapName:String, kvmmName:String,valCountName:String, indexKeyCountName:String, keyHash:FiveInt, keyCBAW:ComparableBAW, keyvalhash:TenInt, valBAW:BAW)  extends Runnable with Serializable with PartitionAware[FiveInt]
 {
   def run()
   {
     val hi            = Hazelcast.getDefaultInstance//newHazelcastInstance()
     hi.getMap[FiveInt,ComparableBAW](keyMapName).putIfAbsent(keyHash, keyCBAW)
     val addedvalmap   = hi.getMap[TenInt,BAW](valMapName).put(keyvalhash, valBAW)
     val addedkvmm     = hi.getMultiMap[FiveInt,TenInt](kvmmName).put(keyHash,keyvalhash)
     if ( addedkvmm != (addedvalmap == null)) println(s"Warning: Bidirindex: addEntry: kvMultiMap's and valMap's put operations did not return equally. Expect inconsistencies! ")//Index name: $name")       // omitting key and value as important information because this would cause both key and value object to be serialized and packed into each Callable!
     if(addedkvmm){
       val valCount  = hi.getAtomicNumber(valCountName).incrementAndGet()
       if (valCount  == 1) hi.getAtomicNumber(indexKeyCountName).incrementAndGet()
     }
   }
   def getPartitionKey = keyHash
 }

 class BiRemoveEntryRunnable (keyMapName:String, valMapName:String, kvmmName:String,valCountName:String, indexKeyCountName:String, keyHash:FiveInt, keyvalhash:TenInt, valBAW:BAW)  extends Runnable with Serializable with PartitionAware[FiveInt]
 {
   def run()
   {
     val hi            = Hazelcast.getDefaultInstance//newHazelcastInstance()
     val removeVM      = hi.getMap[TenInt,BAW](valMapName).remove(keyvalhash, valBAW)
     val removedKVMM   = hi.getMultiMap[FiveInt,TenInt](kvmmName).remove(keyHash,keyvalhash)

     if (removedKVMM != removeVM)  println(s"Warning: Bidirindex: removeEntry: kvMultiMap's and valMap's remove operations did not return equally.")// Expect inconsistencies! Index name: $name")    // omitting key and value as important information because this would cause both key and value object to be serialized and packed into each Callable!

     if(removedKVMM){
       val valCount = hi.getAtomicNumber(valCountName).decrementAndGet()
       if (valCount == 0) {
         hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
         hi.getMap[FiveInt,ComparableBAW](keyMapName).remove(keyHash)
       }
     }

   }
   def getPartitionKey = keyHash
 }


 class BiRemoveAllEntriesRunnable (keyMapName:String, valMapName:String, kvmmName:String,valCountName:String, indexKeyCountName:String, keyHash:FiveInt)  extends Runnable with Serializable with PartitionAware[FiveInt]
 {
   def run()
   {
     val hi          = Hazelcast.newHazelcastInstance()

     hi.getMap[FiveInt,ComparableBAW](keyMapName).remove(keyHash)
     val localvalmap = hi.getMap[TenInt,BAW](valMapName)
     val localkvmm = hi.getMultiMap[FiveInt, TenInt](kvmmName)

     val valuesAssociatedWithKey = localkvmm.get(keyHash).iterator()

     while(valuesAssociatedWithKey.hasNext)
     {localvalmap.remove(valuesAssociatedWithKey.next())}

     val removedKVMM = localkvmm.remove(keyHash).size()      // hmm. instead of boolean, remove actually returns the old collection contained in! That's okish for kvmm since it's only hashes.
     if(removedKVMM > 0)
     {
       hi.getAtomicNumber(valCountName).destroy()
       hi.getAtomicNumber(indexKeyCountName).decrementAndGet()
     }
   }
   def getPartitionKey = keyHash
 }

} */
