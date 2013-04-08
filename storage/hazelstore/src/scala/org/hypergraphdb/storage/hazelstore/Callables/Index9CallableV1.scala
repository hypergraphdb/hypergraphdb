package org.hypergraphdb.storage.hazelstore.Callables

import org.hypergraphdb.storage.hazelstore.Common.{TenInt, FiveInt}
import org.hypergraphdb.storage.hazelstore.ComparableBAW
import java.util.concurrent.Callable
import com.hazelcast.core.{Hazelcast, PartitionAware}

object Index9CallableV1 {


  class AddEntry9Callable(keyMapName:String,valMapName:String, kvsetName:String, valCountName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW,valHash:TenInt, valBA:ComparableBAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def call() = {
      val hi              = Hazelcast.getDefaultInstance
      val localKeyMap     = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val localValueMap   = hi.getMap[TenInt, ComparableBAW](valMapName)
      val localKVSet      = hi.getSet[TenInt](kvsetName)
      val localValueCount = hi.getAtomicNumber(valCountName)
      val localKeyCount   = hi.getAtomicNumber(keyCountName)

      localKeyMap.put(keyHash, keyBA)
      localValueMap.put(valHash, valBA)
      val added = localKVSet.add(valHash)
      if(added){
        val valCount = localValueCount.incrementAndGet()
        if (valCount ==1) localKeyCount.incrementAndGet()
      }
      Unit
    }
  }



  class RemoveEntry9Callable(keyMapName:String,valMapName:String, kvsetName:String, valCountName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW,valHash:TenInt, valBA:ComparableBAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
      val hi              = Hazelcast.getDefaultInstance
      val localKeyMap     = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val localValueMap   = hi.getMap[TenInt, ComparableBAW](valMapName)
      val localKVSet      = hi.getSet[TenInt](kvsetName)
      val localValueCount = hi.getAtomicNumber(valCountName)
      val localKeyCount   = hi.getAtomicNumber(keyCountName)
      localValueMap.remove(valHash, valBA)
      localKVSet.remove(valHash)
      val valCount = localValueCount.decrementAndGet()
      if (valCount  == 0)
      {
        localKeyMap.remove(keyHash, keyBA)
        localKeyCount.decrementAndGet()
      }
      Unit
    }

    def getPartitionKey = keyHash
  }





  class RemoveAllEntries9Callable(keyMapName:String,valMapName:String, kvsetName:String, valCountName:String,keyCountName:String,keyHash:FiveInt,keyBA:ComparableBAW,valHash:TenInt, valBA:ComparableBAW) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
      val hi              = Hazelcast.getDefaultInstance
      val localKeyMap     = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val localValueMap   = hi.getMap[TenInt, ComparableBAW](valMapName)
      val localKVSet      = hi.getSet[TenInt](kvsetName)
      val localValueCount = hi.getAtomicNumber(valCountName)
      val localKeyCount   = hi.getAtomicNumber(keyCountName)
      localKeyMap.remove(keyHash)
      val kvIt = localKVSet.iterator()
      while(kvIt.hasNext){
        val current = kvIt.next()
        localValueMap.remove(current)
      }
      localKVSet.destroy()
      localValueCount.set(0)       //  // destroy leads to weird Hazelcast issues,
      // basically, it would be necessary to revalidate in each operation somehow
      // TODO - This AtomicNumber never gets removed!?

      val keyCount = localKeyCount.decrementAndGet()  // localKeyCount.get was 0 before decrement! so, we get -1 here afterwards!
      if(keyCount == -1)
        localKeyCount.set(0)
      Unit
    }

    def getPartitionKey = keyHash
  }

}
