package storage

import org.hypergraphdb.HGRandomAccessResult

import scala.util.Using
import scala.util.Random
import org.hypergraphdb.HGStore
import org.hypergraphdb.HGConfiguration
import org.hypergraphdb.storage.HGStoreImplementation
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import org.hypergraphdb.HGPersistentHandle
import org.hypergraphdb.HGLink
import org.hypergraphdb.HGPlainLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.hypergraphdb.util.HGUtils
import org.scalatest.OptionValues
import org.scalatest.Inside
import org.scalatest.Inspectors
import org.scalatest.BeforeAndAfterAll
import org.scalatest.*
import flatspec.*
import matchers.*
import org.hypergraphdb.HGSearchResult
// import org.hypergraphdb.storage.bje.BJEConfig

import scala.collection.mutable.ArrayBuffer
import collection.convert.ImplicitConversions.*
import collection.convert.ImplicitConversionsToScala.*
import java.util.UUID

object ToDebug extends Tag("ToDebug")

trait StorageTestEnv extends should.Matchers 
                     with Suite
                     with OptionValues 
                     with Inside 
                     with Inspectors
                     with BeforeAndAfterAll {
  val random = new Random()                                              
  var storeOption: Option[HGStore] = None
  var config:HGConfiguration =  null
  var storeImplementation:HGStoreImplementation = null

  // test framework stuff
  protected def info: Informer

  protected def withNewConfig(config: ConfigMap, args: Args): Args = Args(
      args.reporter,
      args.stopper,
      args.filter,
      config,
      args.distributor,
      args.tracker,
      args.chosenStyles,
      args.runTestInNewInstance,
      args.distributedTestSorter,
      args.distributedSuiteSorter
  )

  // HGDB stuff
  def storeImplementationClass = {
    // "org.hypergraphdb.storage.bje.BJEStorageImplementation"
//    "org.hypergraphdb.storage.lmdb.StorageImplementationLMDB"
    "org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB"
  }                                              

  def baseDatabaseLocation = {
    "/tmp/hgdb_storage_tests"
  }

  def freshDatabaseLocation(): String = {
    baseDatabaseLocation + "/" + UUID.randomUUID().toString()
  }

  val databaseLocation = freshDatabaseLocation()

  def tx[T](f: => T) = { //, onRetry: Option[Runnable] = None) = {
    // val config = new HGTransactionConfig()
    // onRetry.map(config.setBeforeConflictRetry(_))
    this.storeOption.map(store => {
      store.getTransactionManager().ensureTransaction(() => f)
    })
  }

  def assertResultContains[T](rs: HGRandomAccessResult[T], item: T): Unit = {
    assert(true)
    Using.resource(rs) { rs => assert(rs.goTo(item, true) == GotoResult.found) }
  }
  
  def assertNotInResult[T](rs: HGRandomAccessResult[T], item: T): Unit = {
    Using.resource(rs) { rs => assert(rs.goTo(item, true) == GotoResult.nothing) }
  }

  def collect[T](rs: HGSearchResult[T]): Iterable[T] = {
    val L = ArrayBuffer[T]()
    Using.resource(rs) { rs => rs.foreach(L.add) }
    L
  }
  
  def newhandle() = config.getHandleFactory.makeHandle

  def randomBytes(count: Int): Array[Byte] = {
    random.nextBytes(count);
  }

  def randomString(maxLength: Int): String = {
    val length = 1 + random.nextInt(maxLength)
    random.nextString(length)
  }

  def handleArray(arity: Int): Array[HGPersistentHandle] = {
    val handles = new Array[HGPersistentHandle](arity)
    for (i <- 0 until arity)
        handles(i) = newhandle()
    handles
  }

  def createLink(arity: Int): HGLink = {
    new HGPlainLink(handleArray(arity): _*)
  }  

  def getStore(enforceTransactions: Boolean = false) = {
    if (!storeOption.isDefined) {
      info("Using storage implementation " + storeImplementationClass)
      config = new HGConfiguration()
      config.setEnforceTransactionsInStorageLayer(enforceTransactions)
      storeImplementation = Class.forName(storeImplementationClass).newInstance.asInstanceOf[HGStoreImplementation]
      // storeImplementation.getConfiguration().asInstanceOf[BJEConfig].setSerializableIsolation(false)
      config.setStoreImplementation(storeImplementation)
      (new java.io.File(databaseLocation)).mkdirs()
      storeOption = Some(new HGStore(databaseLocation, config))
    }
    storeOption.get
  }

  override def afterAll() = {
    storeOption.foreach(_.close())
    info("Dropping database " + databaseLocation)
    HGUtils.dropHyperGraphInstance(databaseLocation)
    info("Database dropped")
  }  
}
