package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{TimeUnit, ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{Transaction, Hazelcast, PartitionAware}
import java.util
import com.hazelcast.partition.{MigrationListener, MigrationEvent}

object IndexCallablesV12 {

  class GetMultiMappingsFromThatMemberMono(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], retryCount:Int) extends Callable[java.util.List[Pair[ComparableBAW,util.Set[BAW]]]] with Serializable{
    def call() = {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMap[FiveInt,java.util.Set[BAW]](kvmmName)
      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[ComparableBAW,util.Set[BAW]]]
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
      val kvmm              = hi.getMap[FiveInt,java.util.Set[BAW]](kvmmName)

      val valSet = kvmm.get(keyHash)
      if(valSet == null || valSet.isEmpty)
        null.asInstanceOf[BAW]
      else
        valSet.iterator().next
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
