package storage

import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.hypergraphdb.HGStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import scala.concurrent.Future
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import storage.help

import scala.reflect.ClassTag
import org.hypergraphdb.storage.BAtoBA
import org.hypergraphdb.HGIndex
import org.scalatest.*
import org.scalactic.source
import org.scalatest.flatspec.AnyFlatSpec
import org.hypergraphdb.transaction.TransactionConflictException
import org.hypergraphdb.util.HGUtils

import scala.collection.mutable.ArrayBuffer

class ConcurrentTransactionsTests  extends AnyFlatSpec 
                                   with StorageTestEnv {
  
  val store: HGStore = getStore(true)

  type FixtureParam = Object
  
  override def withFixture(test: NoArgTest) = {
    try {      
      val fixture = Object()
      info("Running test " + test.name + " with tags " + test.tags)
      val idx: HGIndex[Array[Byte], Array[Byte]] = store.getIndex(StoreOperation.indexName(), BAtoBA.getInstance(), BAtoBA.getInstance(), null, null, true)
      info(s"Created index ${idx.getName()}")
      super.withFixture(test) // Invoke the test function
    }
    finally {
      // this.store.removeIndex(StoreOperation.indexName())
      println("index removed")
    }
  }

  def waitAndCheckSuccess[T](futures: IterableOnce[Future[T]]): Unit = {
    val exceptions: List[Throwable] = Await.result(help.allOf(futures).map(_.foldLeft(List[Throwable]()) {
      (exceptions, result) => result match {
        case Success(_) => exceptions
        case Failure(e) => e :: exceptions
      }}), Duration.Inf)

    exceptions.foreach(_.printStackTrace())
    exceptions.headOption.map(throw _)
  }

  it should "store multiple entries concurrently in index"  in { // (fixture: FixtureParam) =>
    val threadCount = 20
    val iterations = 100
    val keySet = scala.collection.mutable.Set[Array[Byte]]()
    1 to threadCount map { _ => keySet.add(randomBytes(10)) }

    val allvalues = scala.collection.mutable.Map[Array[Byte], List[Array[Byte]]]()
    1 to iterations map { iteration =>
      keySet map { key => {
        val values = 1 to (5 + random.nextInt(15)) map { size => randomBytes(10 + random.nextInt(40)) }
        assert(values(0) !== values(1))
        allvalues.put(key, values.toList)
      }}
      val futures = keySet map { key => Future {
        val idx: HGIndex[Array[Byte], Array[Byte]] = store.getIndex(StoreOperation.indexName())
        val values = allvalues.get(key).get
        tx({
           assert(idx.findFirst(key) == null)
           values.foreach(v => idx.addEntry(key, v))
           collect(idx.find(key)).size should equal(values.size)
        })

        tx({
          var stored = collect(idx.find(key))
          stored.size should equal(values.size)
          stored.toSet should equal(values.toSet)
        })
      }}

      waitAndCheckSuccess(futures)

      val checkfutures = keySet map { key1 => Future {
        val idx: HGIndex[Array[Byte], Array[Byte]] = store.getIndex(StoreOperation.indexName())
        keySet foreach { key =>
          tx({
            val stored = collect(idx.find(key))
            val values = allvalues.get(key).get
            stored.size should equal(values.size)
          })
        }
      }}

      waitAndCheckSuccess(checkfutures)

//      println(s"Iteration ${iteration}")
//      println("Recreate index")

      store.removeIndex(StoreOperation.indexName())
      store.getIndex(StoreOperation.indexName(), BAtoBA.getInstance(), BAtoBA.getInstance(), null, null, true)        
    }
  }

  it should "add storage data and links concurrently" in {  // (fixture: FixtureParam) =>
    val clients = 1 to 20 map {_ => {
      val opcount = 1000
      new StoreClient().operations(Random.shuffle(
         StoreOperation.make[AddData](opcount) ++ StoreOperation.make[RemoveData](opcount)
        ++  StoreOperation.make[IndexAddData](opcount) ++ StoreOperation.make[IndexRemoveData](opcount)
        ++ StoreOperation.make[IndexAddMultipleData](opcount) ++  StoreOperation.make[IndexRemoveMultipleData](opcount)
      ))
    }}

    val all: Future[Seq[Try[Unit]]] = help.allOf[Unit](clients.map(client => Future { client.execute(this) } ))

    val exceptions: List[Throwable] = Await.result(all.map(_.foldLeft(List[Throwable]()) { 
      (exceptions, result) => result match { 
      case Success(_) => exceptions
      case Failure(e) => e :: exceptions
    }}), Duration.Inf)

    exceptions.foreach(_.printStackTrace())
    exceptions.headOption.map(throw _)
  }
}
