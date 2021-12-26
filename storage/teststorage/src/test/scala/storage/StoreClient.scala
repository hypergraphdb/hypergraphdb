package storage

import scala.collection.mutable.ArrayBuffer

class StoreClient {

  val operations = ArrayBuffer[StoreOperation]()

  def operations(ops: IndexedSeq[StoreOperation]): StoreClient = {
    operations.addAll(ops)
    this
  }

  def execute(env:StorageTestEnv): Unit = {
    operations.foreach(_.prepare(env))
    operations.foreach(_.perform())
    operations.foreach(_.verify())
  }
}
