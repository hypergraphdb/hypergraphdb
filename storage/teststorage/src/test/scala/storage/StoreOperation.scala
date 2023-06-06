package storage

import org.hypergraphdb.HGStore
import org.hypergraphdb.HGHandle
import scala.reflect.ClassTag
import scala.util.Random
import org.hypergraphdb.HGIndex
import scala.collection.mutable
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers._
import org.scalactic.Equality


// this is needed in order to deep compare collections of array. since we 
// are working with byte buffer (e.g. arrays) and sets or lists of those
// this comes up quite often, unfortunately this is a design limitation
// as per https://github.com/scalatest/scalatest/issues/491 (there was some intention
// to put in 3.0, but didn't happen)
//
// So with this we can have two values of type Set[Array[Byte]], i.e. two sets
// of row data and compare them with assert(x === y) or x should equal(y)
implicit def customEq[S: ClassTag]: Equality[Set[S]] = new Equality[Set[S]] {

  override def areEqual(left: Set[S], right: Any): Boolean = {
    val equal = right match {
      case r: Set[_] if implicitly[ClassTag[S]].runtimeClass.isArray =>
        val sameSize = left.size == r.size
        val sameContent = {
          val leftHashes: Set[Int] = left.map {_.asInstanceOf[Array[_]].toSeq.hashCode()}
          val rightHashes: Set[Int] = r.map {_.asInstanceOf[Array[_]].toSeq.hashCode()}
          leftHashes == rightHashes
        }
        sameSize && sameContent
      case _ => left == right
    }
    equal
  }

}

trait StoreOperation {
  def prepare(env: StorageTestEnv): StoreOperation
  def perform():Unit
  def verify():Unit

}

object StoreOperation {

  def indexName() = "indexForOperations"

  def make[T: ClassTag](n: Int): IndexedSeq[T] = 
    (1 to n  map { _ => implicitly[ClassTag[T]].runtimeClass.newInstance.asInstanceOf[T]})
}

import StoreOperation.indexName

abstract class StoreOperationBase extends storage.StoreOperation {

  protected var env: StorageTestEnv = null

  def tx[T](f: HGStore => T) = { //, onRetry: Option[Runnable] = None) = {
    // val config = new HGTransactionConfig()
    // onRetry.map(config.setBeforeConflictRetry(_))
    env.storeOption.map(store => store.getTransactionManager().ensureTransaction(() => f(store)))
  }

  def prepare(env: StorageTestEnv): StoreOperation = {
    this.env = env
    this.env.storeOption.getOrElse(
      throw new IllegalArgumentException("The store of the test env must be initialized."))
    this
  }
}


class AddData extends storage.StoreOperationBase {

  var bytes: Array[Byte] = null
  var handle: Option[HGHandle] = None

  def perform():Unit = {
    bytes = env.randomBytes(100)
    tx(s => handle = Some(s.store(bytes)))
  }

  
  def verify():Unit = {
    tx(s => handle.map(h => assert(s.getData(h.getPersistent) != null)))
  }
}


class RemoveData extends storage.StoreOperationBase {

  var bytes: Array[Byte] = null
  var handle: Option[HGHandle] = None

  override def prepare(env: StorageTestEnv): StoreOperation = {
    super.prepare(env)
    tx(store => handle = Some(store.store(env.randomBytes(57))))
    this
  }
  def perform():Unit = {
    tx(s => handle.map(h => s.removeData(h.getPersistent())))
  }

  
  def verify():Unit = {
    tx(s => handle.map(h => assert(s.getData(h.getPersistent) == null)))
  }
}


class IndexAddData extends storage.StoreOperationBase {

  var key: Option[Array[Byte]] = None
  var value: Option[Array[Byte]] = None

  def perform():Unit = {
    key = Some(env.randomBytes(100))
    value = Some(env.randomBytes(50))
    tx(s => s.getIndex(indexName()).addEntry(key.get, value.get))
  }

  
  def verify():Unit = {
    tx(s => key.map(
      keyBytes => s.getIndex[Array[Byte], Array[Byte]](indexName()).findFirst(keyBytes) 
        should equal(value.get))
    )
  }
}

class IndexRemoveData extends storage.StoreOperationBase {

  var key: Option[Array[Byte]] = None
  var value: Option[Array[Byte]] = None

  override def prepare(env: StorageTestEnv): StoreOperation = {
    super.prepare(env)
    key = Some(env.randomBytes(100))
    value = Some(env.randomBytes(50))
    tx(s => s.getIndex[Array[Byte], Array[Byte]](indexName()).addEntry(key.get, value.get))
    this
  }
  def perform():Unit =
    tx(s => key.map(keydata => s.getIndex(indexName()).removeEntry(keydata, value.get)))
  
  def verify():Unit =
    assert(key.isDefined)
    assert(key.get != null)
    tx(s => key.map(keydata => s.getIndex(indexName()).findFirst(keydata) shouldBe null))
}

class IndexAddMultipleData extends storage.StoreOperationBase {

  var key: Option[Array[Byte]] = None
  var values: Option[IndexedSeq[Array[Byte]]] = None

  def perform():Unit = {
    key = Some(env.randomBytes(100))    
    values = Some(
      1 to (5 + env.random.nextInt(15)) map { size => env.randomBytes(10 + env.random.nextInt(40)) }
    )
    tx(s => {
      val idx: HGIndex[Array[Byte], Array[Byte]] = s.getIndex(indexName())
      values.get map { v => idx.addEntry(key.get, v) }
    })
  }

  
  def verify():Unit = {
    tx(s => {
      val idx: HGIndex[Array[Byte], Array[Byte]] = s.getIndex(indexName())      
      key.map(keyBytes => {
        env.collect(idx.find(keyBytes)).toSet should equal(values.get.toSet)
      })
    })
  }
}

class IndexRemoveMultipleData extends storage.StoreOperationBase {

   var key: Option[Array[Byte]] = None
   var values: Option[IndexedSeq[Array[Byte]]] = None
   var key_keep_some: Option[Array[Byte]] = None
   var values_keep_some: Option[IndexedSeq[Array[Byte]]] = None
  
  override def prepare(env: StorageTestEnv): StoreOperation = {
    super.prepare(env)
     values_keep_some = Some(
       1 to (5 + env.random.nextInt(15)) map { size => env.randomBytes(10 + env.random.nextInt(40)) }
     )
    this
  }

  def perform():Unit = {        
    val key = Some(env.randomBytes(100))
    key_keep_some = Some(env.randomBytes(100))
    val values = Some(
      1 to (5 + env.random.nextInt(15)) map { size => env.randomBytes(10 + env.random.nextInt(40)) }
    )

    tx(s => {
      val idx: HGIndex[Array[Byte], Array[Byte]] = s.getIndex(indexName())
      values.get map { v => idx.addEntry(key.get, v) }
      values_keep_some.get map { v => idx.addEntry(key_keep_some.get, v) }
    })

    tx(s =>  {
      val idx: HGIndex[Array[Byte], Array[Byte]] = env.storeOption.get.getIndex(indexName())
      key.map(keydata => idx.removeAllEntries(keydata))
      idx.removeAllEntries(key.get)
      assert(idx.findFirst(key.get) == null)
      key_keep_some.map(keydata => idx.removeEntry(keydata, values_keep_some.get.head))
    })
  }

  def verify():Unit = {
     tx(s => key.map(keydata => assert(
       s.getIndex[Array[Byte], Array[Byte]](indexName()).findFirst(keydata) == null)))
     tx(s => {
       val idx: HGIndex[Array[Byte], Array[Byte]] = s.getIndex(indexName())
       key_keep_some.map(keydata =>
         env.collect(idx.find(keydata)) should not contain(values_keep_some.get.head))
     })
  }
}