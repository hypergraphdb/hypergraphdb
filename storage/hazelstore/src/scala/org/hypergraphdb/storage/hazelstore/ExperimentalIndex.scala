package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{Hazelcast, PartitionAware}
import com.hazelcast.partition.{MigrationEvent, MigrationListener}

import collection.JavaConversions._

/**
 * User: Ingvar Bogdahn
 * Date: 27.04.13
 */
object ExperimentalIndex {
  class AddEntryMono2(keyMapName:String, kvmmName:String, valCountMapName:String,keyCountName:String,
                      keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                      timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def call() {
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
        val fun = hi.getConfig.getProperties.get("keyMap").asInstanceOf[ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]]]
        if (fun == null)
        {
          val b = new ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]] // (Comparator)
          hi.getConfig.getProperties.put("keyMap", b)
          b
        }
        else
          fun
      }

      val list = nvm.get(keyBA)
      val newList:java.util.List[BAW] =
        if(list == null || list.isEmpty)
        {
          val createdList = new java.util.LinkedList[BAW]()
          createdList.add(valBA)
          createdList
        }
        else {
          list.add(valBA)
          list
        }
      nvm.put(keyBA,newList)

      val partitionService = hi.getPartitionService

      val migrationListener = new MigrationListener {
        def migrationStarted(p1: MigrationEvent) {
          val partitionId = p1.getPartitionId
          val iterator = nvm.entrySet().iterator()
          iterator.foreach( mapping => if (partitionService.getPartition(mapping.getKey).getPartitionId == partitionId) iterator.remove())
          //continue here



        }

        def migrationCompleted(p1: MigrationEvent) {}

        def migrationFailed(p1: MigrationEvent) {}
      }

      partitionService.addMigrationListener(migrationListener)


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


}
