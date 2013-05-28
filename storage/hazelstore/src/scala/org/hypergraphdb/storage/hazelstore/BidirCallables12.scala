package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import com.hazelcast.core._
import java.util
import java.util.concurrent.{TimeUnit, Callable}
import scala.Some

object BidirCallables12 {





  class RemoveAllOnMember(valMapName:String, vkmmName:String, valHashs:Iterable[FiveInt],useTransactionalInCallables:Boolean,retryCount:Int) extends Callable[Unit] with Serializable {
    def call() {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()

      //      val keyMap          = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val valMap  = hi.getMap[FiveInt, BAW](valMapName)
      val vkmm    = hi.getMap[FiveInt,java.util.Set[FiveInt]](vkmmName)
      val it      = valHashs.iterator

      var redo:Int       = retryCount
      while(redo > 0)
      {
        val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
      txnOption.map(opt => opt.begin())

      try
      {
        while (it.hasNext){
          val cur = it.next()
          valMap.remove(cur)
          vkmm.remove(cur)
        }

        txnOption.map(_.commit())
//        if (redo < retryCount)
//          println(s"Bi-RemoveAllEntry $ID  succeeded after rollback")
        redo = 0
      }
      catch {
        case e: Throwable => {
          e.printStackTrace()
  //        println("\n\nROLLBACK in Index.removeAll\n\n")
          try {
            txnOption.map(_.rollback())
          }
          catch {
            case e: Throwable => println("\nW A R N I N G: bi-remove ID $ID : Rollback failure!\n"); e.printStackTrace()
          }
          redo = redo - 1
//          if (redo > 0) println(s"\n >>> bi-remove ID $ID  Retrying...") else
            if (redo <= 0) println(s"\n !!! W A R N I N G  bi-remove ID $ID  F A I L E D!  GIVING UP ...")
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
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID = System.nanoTime()

      val map = hi.getMap[FiveInt,R](mapName)
//      println("^^^^ bi-GetItFromThatMember ^^^^")
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
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val map = hi.getMap[FiveInt,R](mapName)
//      println("bi-GetItFromThatMemberPairedWithHash")
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
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      hi.getMap[FiveInt, java.util.Set[FiveInt]](vkmmName).get(valHash).size
    }
    def getPartitionKey = valHash
  }
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
  // this is done as callable using Executor service, since this way, it's not necessary to transfer all vkmm.get(valHash) over the wire just to get it's size
  class FindFirstByValueKeyHash(vkmmname: String, keyMapName:String, valHash:FiveInt) extends Callable[Option[FiveInt]] with PartitionAware[FiveInt] with Serializable {
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val vkmm = hi.getMap[FiveInt, java.util.Set[FiveInt]](vkmmname)
//      println("bi-FindFirstByValueKeyHash")
      val iter = vkmm.get(valHash).iterator()
      if (iter.hasNext)
        Some(iter.next)
      else
        None
    }
    def getPartitionKey = valHash
  }
  /*---------------------------------------------------------------------------------------------------------------------------------------------------------*/

  class GetValHashsForEachKeyHash(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], timeOut:Long) extends Callable[util.List[Pair[ComparableBAW,util.Set[FiveInt]]]] with Serializable{
    def call() = {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi                = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()

      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMap[FiveInt,java.util.Set[FiveInt]](kvmmName)
//      println("bi-GetValHashsForEachKeyHash")
      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[ComparableBAW,util.Set[FiveInt]]]
      while(keyHashsIterator.hasNext)
      {
        val cur       = keyHashsIterator.next()
        resultList.add(Pair(keyMap.get(cur), kvmm.get(cur)))
      }
      resultList
    }
  }
}
