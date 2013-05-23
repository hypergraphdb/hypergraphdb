package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import com.hazelcast.core._
import java.util.concurrent.Callable
import org.hypergraphdb.storage.hazelstore.testing.TestCommons.log


/**
 * User: Ingvar Bogdahn
 * Date: 25.04.13
 */
object RunnableBackbone {
  import Common.O


  case class BiIndexStringParams  (kvmmBiName:String, vkmmName:String, valMapName:String,firstValMapName:String,valHash:FiveInt)
  //case class BiIndexParams  (kvmmBi:MultiMap[FiveInt,FiveInt], vkmm:MultiMap[FiveInt,FiveInt], valMap:IMap[FiveInt, BAW],firstValMap:IMap[FiveInt, BAW],valHash:FiveInt)
  case class BiIndexParams  (kvmmBi:IMap[FiveInt,java.util.Set[FiveInt]], vkmm:IMap[FiveInt,java.util.Set[FiveInt]], valMap:IMap[FiveInt, BAW],firstValMap:IMap[FiveInt, BAW],valHash:FiveInt)

  class Calloppe( indexAndOperationNames:(String,String), keyMapName:String,  keyHash:FiveInt, keyCountName:String,keyBAOp:O[ComparableBAW],  valBAOp:O[BAW], valCountMapName:String,
                  params:Either[String,BiIndexStringParams],
//                  fun: (IMap[FiveInt, ComparableBAW], FiveInt, String, IMap[FiveInt,Long],Either[MultiMap[FiveInt,BAW],BiIndexParams], (String, String,FiveInt,Long)) => (Boolean,String),
                  fun: (IMap[FiveInt, ComparableBAW], FiveInt, String, IMap[FiveInt,Long],Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams], (String, String,FiveInt,Long)) => (Boolean,String),
                  postfun: (HazelcastInstance,String) => Unit,
                  transactionalRetryCount:Int, useTransactionalCallables:Boolean
                  )
    extends Callable[Unit] with Serializable with PartitionAware[FiveInt] {
    def getPartitionKey = keyHash
    def call() {
      val hit = Hazelcast.getAllHazelcastInstances.iterator()
      val hi  = if (hit.hasNext) hit.next else Hazelcast.newHazelcastInstance()
      val ID  = ( indexAndOperationNames._1, indexAndOperationNames._2, keyHash, System.nanoTime())

      val keyMap          = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val valCountMap     = hi.getMap[FiveInt,Long](valCountMapName)

      val funParams:Either[IMap[FiveInt,java.util.Set[BAW]],BiIndexParams] = params
        .left.map(hi.getMap[FiveInt,java.util.Set[BAW]](_))
        .right.map(i => BiIndexParams(
        kvmmBi      = hi.getMap[FiveInt,java.util.Set[FiveInt]](i.kvmmBiName),
        vkmm        = hi.getMap[FiveInt,java.util.Set[FiveInt]](i.vkmmName),
        valMap      = hi.getMap[FiveInt,BAW](i.valMapName),
        firstValMap = hi.getMap[FiveInt, BAW](i.firstValMapName),
        valHash     = i.valHash
      ))

      var redo:Int       = transactionalRetryCount

      while(redo >0)
      {
        val txnOption:Option[Transaction] = if(useTransactionalCallables) Some(hi.getTransaction) else None
        txnOption.map(opt => opt.begin())

        try
        {
          val arg2 = fun(keyMap,keyHash,keyCountName,valCountMap,funParams,ID)
          txnOption.map(_.commit())

          if (redo < transactionalRetryCount)
            log(s"ID $ID succeeded after rollback")

          redo = 0

          if (arg2._1) postfun(hi,arg2._2)

        }
        catch
          { case e:Throwable =>
          {
            e.printStackTrace()
            log(s"Rollback in $ID")
            try    { txnOption.map(_.rollback()) }
            catch  { case e:Throwable => log(s"\n\n W A R N I N G : Rollback failure in ID $ID"); e.printStackTrace()  }
            redo = redo -1
            if(redo >0)
              log(s"Retrying ID $ID")
            else
              log(s"\n\n W A R N I N G : Giving up ID $ID !")
          }
          }
      }
    }
  }

}