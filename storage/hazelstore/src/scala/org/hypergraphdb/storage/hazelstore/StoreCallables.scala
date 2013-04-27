package org.hypergraphdb.storage.hazelstore

import java.util.concurrent.{TimeUnit, Callable}
import com.hazelcast.core.{IMap, PartitionAware, Hazelcast}
import org.hypergraphdb.HGPersistentHandle


object StoreCallables {

  class RemoveIncidenceSetOp(inciDBName: String, inciCountName:String, key:HGPersistentHandle,tryCount:Int) extends Runnable with PartitionAware[HGPersistentHandle]  with Serializable{
    def run {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB        = hi.getMultiMap[HGPersistentHandle, BAW](inciDBName)
      val inciCount     = hi.getMap[HGPersistentHandle,Long](inciCountName)
      println(s"Store - RemoveIncidenceSetOp $inciDBName key: $key")

      var tryAgain = tryCount
      while (tryAgain >0) {
        val t = hi.getTransaction
        try {
          t.begin()
          inciDB.remove(key)
          inciCount.remove(key)
          t.commit()
          tryAgain = 0
        }
        catch { case d: Throwable => {
          t.rollback();
          tryAgain = tryAgain -1
        }
        }
      }
    }
    def getPartitionKey = key
  }

  class AddIncidenceLinkOp(inciDBName: String, inciCountName:String, key:HGPersistentHandle, value:BAW,tryCount:Int) extends Runnable with  PartitionAware[HGPersistentHandle]  with Serializable{
    def run {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB        = hi.getMultiMap[HGPersistentHandle, BAW](inciDBName)
      val inciCount     = hi.getMap[HGPersistentHandle,Long](inciCountName)

      val opId = System.nanoTime()
      println(s"Store - AddIncidenceLinkOp $inciDBName key: $key opID: $opId starting")

      var tryAgain = tryCount
      while (tryAgain >0)
      {
        println(s"Store - AddIncidenceLinkOp $inciDBName key: $key opID: $opId entering whileloop")

        val t = hi.getTransaction
        try{
          t.begin()

          val added = inciDB.put(key,value)

          if (added)
          {
            val oldCount = inciCount.get(key)
            inciCount.put(key, oldCount+1)
            println(s"Store - AddIncidenceLinkOp $inciDBName key: $key opID: $opId incremented Count")
          }
          t.commit()
          tryAgain = 0
          println(s"Store - AddIncidenceLinkOp $inciDBName key: $key opID: $opId committed")
        }

        catch { case d: Throwable => {
          println("Rolling back in AddIncidence")
          t.rollback();
          tryAgain = tryAgain -1
        }}}}

    def getPartitionKey = key
  }


  class RemoveIncidenceLinkOp(inciDBName: String, inciCountName:String, key:HGPersistentHandle, value:BAW,tryCount:Int) extends Runnable with PartitionAware[HGPersistentHandle] with Serializable{
    def run {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val inciDB        = hi.getMultiMap[HGPersistentHandle, BAW](inciDBName)
      val inciCount     = hi.getMap[HGPersistentHandle,Long](inciCountName)

      val opId = System.nanoTime()
      println(s"Store - RemoveIncidenceLinkOp $inciDBName key: $key opID: $opId starting")
      var tryAgain = tryCount
      while (tryAgain >0) {
        println(s"Store - RemoveIncidenceLinkOp $inciDBName key: $key opID: $opId entering while loop")

        val t = hi.getTransaction
        try{
          t.begin()
          //          lock.tryLock(1000,TimeUnit.MILLISECONDS)
          val removed = inciDB.remove(key,value)
          if (removed)
          {
            val oldCount = inciCount.get(key)
            inciCount.put(key, oldCount-1)
            println(s"Store - RemoveIncidenceLinkOp $inciDBName key: $key opID: $opId count decrease")
          }
          t.commit()
          tryAgain = 0
          println(s"Store - RemoveIncidenceLinkOp $inciDBName key: $key opID: $opId committed")
        }
        catch { case d: Throwable => {
          println(s"Store - RemoveIncidenceLinkOp $inciDBName key: $key opID: $opId rollback")
          t.rollback();
          tryAgain = tryAgain -1
        }}}}

    def getPartitionKey = key
  }





  // FOR TESTING
  class AddCallable[T](key:T) extends Callable[Unit] with PartitionAware[T] with Serializable{
    def call() {
      val hi = Hazelcast.getAllHazelcastInstances.iterator().next()
      val map = hi.getMap[T,String]("callable")
      println("callable adding data")
      map.put(key, key + "value")
    }
    def getPartitionKey = key
  }
  class AddRunnable[T](key:T) extends Runnable with PartitionAware[T] with Serializable{
    def run() {
      val hi = Hazelcast.getAllHazelcastInstances.iterator().next()
      val map = hi.getMap[T,String]("runnable")
      println("runnable adding data")
      map.put(key, key + "value")
    }
    def getPartitionKey = key
  }

}
