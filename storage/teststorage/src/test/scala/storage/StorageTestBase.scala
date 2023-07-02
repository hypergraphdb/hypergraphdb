package storage

import org.scalatest._
import flatspec._
import matchers._
import org.hypergraphdb.storage.HGStoreImplementation
import org.hypergraphdb.HGConfiguration
import org.hypergraphdb.HGStore
import org.hypergraphdb.HGLink
import org.hypergraphdb.HGPlainLink
import org.hypergraphdb.util.HGUtils
import scala.util.Random
import org.hypergraphdb.HGHandle
import org.hypergraphdb.HGLink
import org.hypergraphdb.HGPersistentHandle
import java.util.concurrent.Callable
import org.hypergraphdb.transaction.HGTransactionConfig
import org.hypergraphdb.HGRandomAccessResult
import scala.compiletime.ops.boolean
import scala.util.Using
import org.hypergraphdb.HGRandomAccessResult.GotoResult

abstract class StorageTestBase extends AnyFlatSpec with should.Matchers 
                                          with OptionValues 
                                          with Inside 
                                          with Inspectors
                                          with BeforeAndAfterAll {
  val random = new Random()                                              
  var storeOption: Option[HGStore] = None
  var config:HGConfiguration =  null
  var storeImplementation:HGStoreImplementation = null

  def storeImplementationClass = {
    // "org.hypergraphdb.storage.bje.BJEStorageImplementation"
    "org.hypergraphdb.storage.lmdb.StorageImplementationLMDB"
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

  def newhandle() = config.getHandleFactory.makeHandle

  def randomBytes(count: Int): Array[Byte] = {
    random.nextBytes(count);
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
