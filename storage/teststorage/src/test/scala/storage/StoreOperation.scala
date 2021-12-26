package storage

import org.hypergraphdb.HGStore
import org.hypergraphdb.HGHandle
import org.scalatest.Assertions._

trait StoreOperation {
  def prepare(env: StorageTestEnv): StoreOperation
  def perform():Unit
  def verify():Unit
}

abstract class StoreOperationBase extends storage.StoreOperation {

  protected var env: StorageTestEnv = null

  // def tx[T](f: => T) = { //, onRetry: Option[Runnable] = None) = {
  //   // val config = new HGTransactionConfig()
  //   // onRetry.map(config.setBeforeConflictRetry(_))
  //   this..map(store => {
  //     store.getTransactionManager().ensureTransaction(() => f)
  //   })
  // }

  def prepare(env: StorageTestEnv): StoreOperation = {
    this.env = env
    this.env.storeOption.getOrElse(
      throw new IllegalArgumentException("The store of the test env must be initialized."))
    this
  }
}


object AddData {
  def makeN(n: Int): IndexedSeq[AddData] = {
    (1 to n  map { _ => new AddData() })
  }
}

class AddData extends storage.StoreOperationBase {

  var bytes: Array[Byte] = null
  var handle: Option[HGHandle] = None

  def perform():Unit = {
    bytes = env.randomBytes(100)
    handle = env.storeOption.map(_.store(bytes))
  }

  
  def verify():Unit = {
    handle.flatMap(h => env.storeOption.map(s => assert(s.getData(h.getPersistent) != null)))
  }
}

class RemoveData extends storage.StoreOperationBase {

  var bytes: Array[Byte] = null
  var handle: Option[HGHandle] = None

  override def prepare(env: StorageTestEnv): StoreOperation = {
    super.prepare(env)
    env.storeOption.map(store => handle = Some(store.store(env.randomBytes(57))))
    this
  }
  def perform():Unit = {
    handle.map(h => env.storeOption.map(_.removeData(h.getPersistent())))
  }

  
  def verify():Unit = {
    handle.flatMap(h => env.storeOption.map(s => assert(s.getData(h.getPersistent) == null)))
  }
}

object RemoveData {
  def makeN(n: Int): IndexedSeq[RemoveData] = {
    (1 to n  map { _ => new RemoveData() })
  }
}