package storage

import org.scalatest.funsuite.AnyFunSuite
import org.hypergraphdb._

class StorageImplementationTests extends StorageTestBase {
  
  val store = createStore()
  
  // test 1
  it should "'double' should handle 0" in {
    val result = 0*0
    assert(result == 0)
  }

}