package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{ConcurrentSkipListSet, ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{EntryListener, Hazelcast, PartitionAware}
import com.hazelcast.partition.{MigrationEvent, MigrationListener}

import collection.JavaConversions._
import java.util.Comparator

/**
 * User: Ingvar Bogdahn
 * Date: 27.04.13
 */
object ExperimentalIndex {
  class AddEntryMono2(keyMapName:String, kvmmName:String, valCountMapName:String,keyCountName:String,
                      keyHash:FiveInt,keyBA:BAW, valBA:BAW, comparator: Comparator[BAW],
                      timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def getPartitionKey = keyHash
    def call() {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next

      val nvm =
      {
        val fun = hi.getConfig.getProperties.get("keyMap").asInstanceOf[ConcurrentSkipListMap[BAW,ConcurrentSkipListSet[BAW]]]
        if (fun == null)
        {
          val b = new ConcurrentSkipListMap[BAW,ConcurrentSkipListSet[BAW]](comparator)
          hi.getConfig.getProperties.put("keyMap", b)
          b
        }
        else
          fun
      }

      val set = nvm.get(keyBA)


        if(set == null || set.isEmpty)
        {
          val created = new ConcurrentSkipListSet[BAW](comparator)
          created.add(valBA)
          nvm.put(keyBA,created)
        }
        else set.add(valBA)


      val partitionService = hi.getPartitionService


      val migrationListener = new MigrationListener {
        def migrationStarted(p1: MigrationEvent) {
          val partitionId = p1.getPartitionId
          val oldOwner = p1.getOldOwner
          val newOwner = p1.getNewOwner
          p1.getSource
          val iterator = nvm.entrySet().iterator()
          val toMigrate= iterator.filter( mapping => partitionService.getPartition(mapping.getKey).getOwner == newOwner)
          iterator.foreach( mapping => if (partitionService.getPartition(mapping.getKey).getPartitionId == partitionId) iterator.remove())
          //continue here



        }

        def migrationCompleted(p1: MigrationEvent) {}

        def migrationFailed(p1: MigrationEvent) {}
      }

      partitionService.addMigrationListener(migrationListener)

    }
  }


}
