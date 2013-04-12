package org.hypergraphdb.storage.hazelstore

import java.util.concurrent.Callable
import com.hazelcast.core.{PartitionAware, Hazelcast}


object StoreCallables {

  class RemoveIncidenceSetOp(inciDBName: String, inciCountName:String, key:Array[Byte]) extends Callable[Unit] with PartitionAware[Array[Byte]] with Serializable{
    def call() {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB    = hi.getMultiMap[Array[Byte], BAW](inciDBName)
      val inciCount = hi.getAtomicNumber(inciCountName)
      inciDB.remove(key)
      inciCount.set(0)
    }
    def getPartitionKey = key
  }

  class AddIncidenceLinkOp(inciDBName: String, inciCountName:String, key:Array[Byte], value:BAW) extends Callable[Unit] with PartitionAware[Array[Byte]] with Serializable{
    def call() {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB = hi.getMultiMap[Array[Byte], BAW](inciDBName)
      val inciCount = hi.getAtomicNumber(inciCountName)

      val added = inciDB.put(key,value)
      if (added)
        inciCount.incrementAndGet()
    }

    def getPartitionKey = key
  }


  class RemoveIncidenceLinkOp(inciDBName: String, inciCountName:String, key:Array[Byte], value:BAW) extends Callable[Unit] with PartitionAware[Array[Byte]] with Serializable{
    def call() {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB = hi.getMultiMap[Array[Byte], BAW](inciDBName)
      val inciCount = hi.getAtomicNumber(inciCountName)

      val removed = inciDB.remove(key,value)
      if (removed)
        inciCount.decrementAndGet()
    }

    def getPartitionKey = key
  }




}
