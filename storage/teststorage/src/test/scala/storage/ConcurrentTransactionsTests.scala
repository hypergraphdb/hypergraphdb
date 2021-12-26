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

class ConcurrentTransactionsTests  extends FixtureAnyFlatSpec with StorageTestEnv {
  
  val store: HGStore = getStore(true)

  type FixtureParam = Object
  
  override def withFixture(test: OneArgTest) = {
    try {      
      val fixture = Object()
      info("Running test " + test.name + " with tags " + test.tags)
      withFixture(test.toNoArgTest(fixture))      
    }
    finally {
    }
  }

  it should "add storage data and links concurrently" in { (fixture: FixtureParam) =>  
    val A = new StoreClient().operations(Random.shuffle(AddData.makeN(100) ++ RemoveData.makeN(100)))
    val B = new StoreClient().operations(Random.shuffle(AddData.makeN(100) ++ RemoveData.makeN(100)))
    val f1 = Future { A.execute(this)  }
    val f2 = Future { B.execute(this) }
    val all: Future[Seq[Try[Unit]]] = help.allOf(f1, f2)
    
    val exceptions: List[Throwable] = Await.result(all.map(_.foldLeft(List[Throwable]()) { (exceptions, result) => result match { 
      case Success(_) => exceptions
      case Failure(e) => e :: exceptions
    }}), Duration.Inf)

    exceptions.headOption.map(throw _)
  }
}
