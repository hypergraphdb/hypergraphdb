package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage.hazelstore.Common.FiveInt
import java.util.concurrent.{TimeUnit, ConcurrentSkipListMap, Callable}
import com.hazelcast.core.{Transaction, Hazelcast, PartitionAware}
import java.util
import com.hazelcast.partition.{MigrationEvent, MigrationListener}

object IndexCallablesV12 {

   class AddEntryMono(keyMapName:String, kvmmName:String,valCountMapName:String,keyCountName:String,
                      keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                      useTransactionalInCallables:Boolean, useHCIndexing: Boolean, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def getPartitionKey = keyHash
     def call(){
         val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
        if(useHCIndexing)
          hi.getConfig.addMapConfig(new com.hazelcast.config.MapConfig(keyMapName).addMapIndexConfig(new com.hazelcast.config.MapIndexConfig("data", true)))

       val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val keyCount    = hi.getAtomicNumber(keyCountName)

       var incrementKeyCount = false
       var redo = 5
       while(redo >0)
       {
       val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
       txnOption.map(opt => opt.begin())
       try
       {
         keyMap.put(keyHash, keyBA)
         val added = kvmm.put(keyHash,valBA)
         if(added)
         {
           val valCountOld = valCountMap.get(keyHash)
           val valCountNew = valCountOld + 1
           valCountMap.put(keyHash, valCountNew)
           if (valCountOld == null || valCountOld == 0)
             incrementKeyCount = true
         }
         else
          println("index:addEntry kvmm put false")
         Unit
       }
       finally{
         try  {
           txnOption.map(_.commit())
           if(incrementKeyCount)
           { println("Index.AddEntryOp: keycount is now being incremented. ")
             keyCount.incrementAndGet()
           }
           redo = 0
         }
         catch { case e:Throwable => {
           e.printStackTrace()
           println("\n\nROLLBACK in Index.addEntry\n\n")
           try {txnOption.map(_.rollback())} catch{case e:Throwable => println("\nWarning: Rollback failure!"); e.printStackTrace()}
           redo = redo -1
           if(redo >0)
             println("\nRetrying...")
         }
         }
       }
     }
     }

   }


  class RemoveEntryMono(keyMapName:String, kvmmName:String, valCountMapName:String,indexKeyCountName:String,
                        keyHash:FiveInt,keyBA:ComparableBAW, valBA:BAW,
                        useTransactionalInCallables:Boolean, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
    def call() {
      val hi                = Hazelcast.getAllHazelcastInstances.iterator().next
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyMap            = hi.getMap[FiveInt, ComparableBAW](keyMapName)
      val keyCount = hi.getAtomicNumber(indexKeyCountName)

      var keyCountDecrease = false

      val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
      txnOption.map(opt => opt.begin())

      try
      {
        val kvmmRemoved    = kvmm.remove(keyHash, valBA)

        if(kvmmRemoved)
        {
          val valCountMap     = hi.getMap[FiveInt, Long](valCountMapName)
          val valCountOld     = valCountMap.get(keyHash)
          if (valCountOld > 0)
          {
            valCountMap.put(keyHash,valCountOld - 1)
            if (valCountOld == 1)
            {
              keyCountDecrease = true
               val removed = keyMap.remove(keyHash) != null
               if (!removed)
                  println("Index11Callable RemoveEntryMono possible inconsistency: keyMap remove returned null where it should not.")
            }
          }
        }
        Unit
      }
      finally{
        try  {
          txnOption.map(_.commit())
          if (keyCountDecrease) {
            println("Index.removeEntry: keyCount is now decremented. ")
            keyCount.decrementAndGet()
          }
        }
        catch { case e:Throwable => {
          e.printStackTrace()
          println("ROLLBACK in Index.addEntry")
          txnOption.map(_.rollback())
        }
        }
      }
    }

     def getPartitionKey = keyHash
   }




  class RemoveAllEntriesMono(keyMapName:String, kvmmName:String,  valCountMapName:String,keyCountName:String,
                             keyHash:FiveInt,
                             useTransactionalInCallables:Boolean, timeOut:Long) extends Callable[Unit] with Serializable with PartitionAware[FiveInt]{
     def call() {

       val hi               = Hazelcast.getAllHazelcastInstances.iterator().next

       val kvmm             = hi.getMultiMap[FiveInt,BAW](kvmmName)
       val keyMap      = hi.getMap[FiveInt, ComparableBAW](keyMapName)
       val valCountMap        = hi.getMap[FiveInt, Long](valCountMapName)
       val keyCount    = hi.getAtomicNumber(keyCountName)

       val txnOption:Option[Transaction] = if(useTransactionalInCallables) Some(hi.getTransaction) else None
       txnOption.map(opt => opt.begin())

       var keyCoundDecrement:Boolean = false
       try
       {
         val a              = kvmm.remove(keyHash)
         val removedKvmm    = a != null && a.size != 0
         val removedKeyMap  = keyMap.remove(keyHash) != null
         valCountMap.put(keyHash,0)

         if(removedKvmm != removedKeyMap )
           println("inconsistency in Index during removeAllEntries. removedKvmm should be equal to removedKeyMap")
         if(removedKvmm && removedKeyMap)
           keyCoundDecrement = true

         Unit
       }
       finally{
         try  {
           txnOption.map(_.commit())
           if (keyCoundDecrement)
           {
             println("Index.removeAllEntries: keyCount is now decremented. ")
             keyCount.decrementAndGet()
           }
         }
         catch { case e:Throwable => {
           e.printStackTrace()
           println("ROLLBACK in Index.addEntry")
           txnOption.map(_.rollback())
            }
          }
       }
     }
     def getPartitionKey = keyHash
   }


  class GetMultiMappingsFromThatMemberMono(keyMapName:String, kvmmName:String, keyHashs:Iterable[FiveInt], timeOut:Long) extends Callable[java.util.List[Pair[ComparableBAW,util.Collection[BAW]]]] with Serializable{
    def call() = {
      val hi               = Hazelcast.getAllHazelcastInstances.iterator().next
      val keyMap            = hi.getMap[FiveInt,ComparableBAW](keyMapName)
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)
      val keyHashsIterator  = keyHashs.iterator
      val resultList        = new util.LinkedList[Pair[ComparableBAW,util.Collection[BAW]]]
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
      val kvmm              = hi.getMultiMap[FiveInt,BAW](kvmmName)

      val valHashIt = kvmm.get(keyHash).iterator()
      if(valHashIt.hasNext)
        valHashIt.next()
      else
        null.asInstanceOf[BAW]
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

/*
//Experimental

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
        val a = hi.getConfig.getProperties.get("keyMap").asInstanceOf[ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]]]
        if (a == null)
        {
          val b = new ConcurrentSkipListMap[ComparableBAW,java.util.List[BAW]] // (Comparator)
          hi.getConfig.getProperties.put("keyMap", b)
          b
        }
        else
          a
      }

      val list = nvm.get(keyBA)
      val newList:java.util.List[BAW] =
        if(list == null || list.isEmpty)
        {
          val createdList = new util.LinkedList[BAW]()
          createdList.add(valBA)
          createdList
        }
        else {
          list.add(valBA)
          list
        }
      nvm.put(keyBA,newList)

      val migrationListener = new MigrationListener {
        def migrationStarted(p1: MigrationEvent) {

        }

        def migrationCompleted(p1: MigrationEvent) {}

        def migrationFailed(p1: MigrationEvent) {}
      }
      Hazelcast.getPartitionService.addMigrationListener(migrationListener)


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
  */

}
