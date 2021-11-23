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
import org.scalatest._
import flatspec._
import matchers._
import org.hypergraphdb.HGSearchResult
import scala.collection.mutable.ArrayBuffer
import collection.convert.ImplicitConversions._
import collection.convert.ImplicitConversionsToScala._

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
    "org.hypergraphdb.storage.bje.BJEStorageImplementation"
  }                                              

  def databaseLocation = {
    "/tmp/hgdb_storage_tests"
  }

  def tx[T](f: => T) = { //, onRetry: Option[Runnable] = None) = {
    // val config = new HGTransactionConfig()
    // onRetry.map(config.setBeforeConflictRetry(_))
    this.storeOption.map(store => {
      store.getTransactionManager().ensureTransaction(() => f)
    })
  }

  def assertResultContains[T](rs: HGRandomAccessResult[T], item: T): Unit = {
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

  def getStore() = {
    if (!storeOption.isDefined) {
        info("Using storage implementation " + storeImplementationClass)
        config = new HGConfiguration()
        config.setEnforceTransactionsInStorageLayer(true)
        storeImplementation = Class.forName(storeImplementationClass).newInstance.asInstanceOf[HGStoreImplementation]
        config.setStoreImplementation(storeImplementation)
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
