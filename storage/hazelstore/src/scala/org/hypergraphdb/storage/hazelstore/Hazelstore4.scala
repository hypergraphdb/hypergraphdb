package org.hypergraphdb.storage.hazelstore

import java.util.Comparator
import com.hazelcast.core._
import collection.JavaConversions._
import com.hazelcast.util.DistributedTimeoutException
import com.hazelcast.nio.HazelcastSerializationException
import _root_.org.hypergraphdb._
import _root_.org.hypergraphdb.util.HGLogger
import storage._
import transaction._
import org.hypergraphdb.`type`.HGHandleType.HandleComparator
import org.hypergraphdb.storage.hazelstore.StoreCallables.{RemoveIncidenceLinkOp, AddIncidenceLinkOp, RemoveIncidenceSetOp}
import java.util.concurrent.Callable


class Hazelstore4 (hazelstoreConf: HazelStoreConfig = new HazelStoreConfig()) extends HGStoreImplementation
{
  type BA = Array[Byte]
  type PH = HGPersistentHandle

  //val hi:HazelcastInstance = hazelstoreConf.getHazelcastInstance.getOrElse(Hazelcast.getDefaultInstance)//if(!hiO.isDefined) Hazelcast.newHazelcastInstance(hazelstoreConf.getHazelConfig) else hiO.get
  //hazelstoreConf.setHazelcastInstance(Some(hi))
  val hi:HazelcastInstance = Hazelcast.newHazelcastInstance(hazelstoreConf.getHazelConfig)

  // according to http://twitter.github.com/effectivescala/  and http://boundary.com/blog/2011/05/  getOrElseUpdate is problematic with ConcurrentHashMap. See second link for alternatives
  protected val openIndices               = new java.util.concurrent.ConcurrentHashMap[String, HGIndex[_,_]](60, 0.8f, 2)

  protected var hgConfig:HGConfiguration  = _

  lazy val handleFactory :HGHandleFactory = hgConfig.getHandleFactory
  protected lazy val handleSize           = handleFactory.nullHandle().toByteArray.size
  val handleByteArrayComparator                    = new HandleComparator
  lazy val handleconverter                = new HandleConverter(handleFactory)

  protected val logger                    = new HGLogger()
  protected val linkDB: IMap[BA, BAW]     = hi.getMap("linkDB")
  protected val dataDB: IMap[BA, BAW]     = hi.getMap("dataDB")
  val inciDbName = "inciDB"
  protected val inciDB: MultiMap[BA, BAW] = hi.getMultiMap[BA, BAW](inciDbName)
  def inciName(handle:PH):String          = "inciCount" + handle.toStringValue
  def inciCount(handle:PH):AtomicNumber   = hi.getAtomicNumber(inciName(handle))
  protected val inciConstant = "inciDB"
  protected val clusterExecutor           = hi.getExecutorService
  val hazelstoreConfig: HazelStoreConfig  = new HazelStoreConfig()


  def getConfiguration                    = hazelstoreConfig

  def startup(store: HGStore, configuration: HGConfiguration) {
    hgConfig = configuration
    if (configuration.isTransactional)
      throw new HGException("Hazelcast-Storage does not support transactions at the moment. Disable with hgconfig.setTransactional(false)")
  }

  def shutdown() { Hazelcast.shutdownAll()  }

  // couldn't be here a simple singleton instead of an anonymous class instantiated each call ?
  def getTransactionFactory =  new HGTransactionFactory {
    def createTransaction(context: HGTransactionContext,
                          config: HGTransactionConfig,
                          parent: HGTransaction):HGStorageTransaction =
      if (parent != null)
        throw new IllegalStateException("Nested transaction detected. Not supported by Hazelstore.")
      else
      {
        println("Creating hazelcast transaction senselessly")
        val transaction = hi.getTransaction
        transaction.begin()
        new HazelTransaction(transaction)
      }

    def canRetryAfter(t: Throwable):Boolean =  t.isInstanceOf[TransactionConflictException] ||                  // ToDo -- unclear whether we should retry at Timeout Exceptions?
                                              (
                                                ! t.isInstanceOf[DistributedTimeoutException] &&
                                                ! t.isInstanceOf[HazelcastSerializationException] &&
                                                ! t.isInstanceOf[InstanceDestroyedException] &&
                                                ! t.isInstanceOf[OperationTimeoutException] &&
                                                ! t.isInstanceOf[RuntimeInterruptedException] &&
                                                ! t.isInstanceOf[ClassCastException]
                                              )
  }

  def store(handle: PH, link: Array[PH]):PH = {
    if(hazelstoreConfig.getAsync)
      linkDB.putAsync(handle.toByteArray, new BAW(pHA2BA(link)));
    else
      linkDB.put(handle.toByteArray, new BAW(pHA2BA(link)));
    handle
  }

  def getLink(handle: PH) : Array[PH] =
    if (handle == null)
      null
    else
      {
        val ret : BAW = linkDB.get(handle.toByteArray)
        if (ret != null)
          ba2PHA(ret.data)
        else
          null
      }

  def removeLink(handle: PH){
    if(hazelstoreConfig.getAsync)
      linkDB.removeAsync(handle.toByteArray)
    else
      linkDB.remove(handle.toByteArray)
  }

  def containsLink(handle: PH):Boolean =
    linkDB.containsKey(handle.toByteArray)

  def store(handle: PH, data: BA) ={
    if(hazelstoreConfig.getAsync)
      dataDB.putAsync(handle.toByteArray, new BAW(data))
    else
      dataDB.put(handle.toByteArray, new BAW(data))

    handle
    }

  def getData(handle: PH) : BA =
    if (handle == null) null.asInstanceOf[BA]
    else {
       val res = dataDB.get(handle.toByteArray)
        if (res != null)
          res.data
        else
          null.asInstanceOf[BA]
    }


  def removeData(handle: PH) {
    if(hazelstoreConfig.getAsync)
      dataDB.removeAsync(handle.toByteArray)
    else
      dataDB.remove(handle.toByteArray)
  }


  def containsData(handle: PH) =  dataDB.containsKey(handle.toByteArray)


  def getIncidenceResultSet(handle: PH) = {
    val map =  inciDB.get(handle.toByteArray).map(baw => baw.data).toIndexedSeq
    val sorted = map.sortWith{case (k1,k2) => handleByteArrayComparator.compare(k1,k2)< 0}
    new HazelRS2[PH](sorted)(handleByteArrayComparator, handleconverter)
    }

  def execute(callable:Callable[Unit]){
    if (hazelstoreConfig.getAsync)
      clusterExecutor.submit(callable)
    else
      callable.call()
  }

  def removeIncidenceSet(handle: PH) {
    val callable = new RemoveIncidenceSetOp(inciDbName, inciName(handle), handle.toByteArray)
    if (hazelstoreConfig.getAsync)
        execute(callable)
    else
      callable.call
  }

  def getIncidenceSetCardinality(handle: PH): Long = inciCount(handle).get()

  def addIncidenceLink(handle: PH, newLink: PH) {
    val callable = new AddIncidenceLinkOp(inciDbName, inciName(handle), handle.toByteArray, new BAW(newLink.toByteArray))
    if (hazelstoreConfig.getAsync)
      execute(callable)
    else
      callable.call
  }

  def removeIncidenceLink(handle: PH, oldLink: PH) {
    val callable = new RemoveIncidenceLinkOp(inciDbName, inciName(handle), handle.toByteArray, new BAW(oldLink.toByteArray))
    if (hazelstoreConfig.getAsync)
      execute(callable)
    else
      callable.call
  }

  def getIndex[K, V](name: String,
                     keyConverter: ByteArrayConverter[K],
                     valueConverter: ByteArrayConverter[V],
                     comparator: Comparator[_],
                     isBidirectional: Boolean,
                     createIfNecessary: Boolean): HGIndex[K, V] =
    openIndices.getOrElseUpdate(name,
                                      (
                                          if (!isBidirectional) new HazelIndex12     [K, V](name, hi, hazelstoreConfig, keyConverter, valueConverter,comparator.asInstanceOf[Comparator[BA]])
                                          else                  new HazelBidirecIndex12[K, V](name, hi, hazelstoreConfig, keyConverter, valueConverter,comparator.asInstanceOf[Comparator[BA]])
                                      )

                                ).asInstanceOf[HGIndex[K, V]]

  def removeIndex(name: String) {
    openIndices.remove(name)
  }

  def ba2ph(ba:BA) = handleFactory.makeHandle(ba)
  def handle2baw(handle:PH) = new BAW(handle.toByteArray)
  def pHA2BA(la:Array[PH]): BA = HGConverter.convertHandleArrayToByteArray(la, handleSize)
  def ba2PHA (ba:BA):Array[PH] = HGConverter.convertByteArrayToHandleArray(ba,handleFactory)

}
