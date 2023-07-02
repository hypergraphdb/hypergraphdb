package storage

import org.hypergraphdb.HGStore
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.hypergraphdb.HGConfiguration
import org.hypergraphdb.util.HGUtils
import org.hypergraphdb.storage.HGStoreImplementation
import org.hypergraphdb.HGException
import org.hypergraphdb.storage.ByteArrayConverter
import org.hypergraphdb.storage.BAtoBA

class TransactionStorageTests extends FixtureAnyFlatSpec with StorageTestEnv {
  
  val store: HGStore = getStore()

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

  it should "throw exception when transactions are enforced but no tx in effect" taggedAs(ToDebug) in { (fixture: FixtureParam) =>  
    val location = freshDatabaseLocation()
    try {
      val config = new HGConfiguration()
      config.setEnforceTransactionsInStorageLayer(true)
      storeImplementation = Class.forName(storeImplementationClass).newInstance.asInstanceOf[HGStoreImplementation]
      config.setStoreImplementation(storeImplementation)
      (new java.io.File(location)).mkdirs()
      val store = new HGStore(location, config)
      assertThrows[HGException](store.store(handleArray(4)))
      store.close()
    }
    finally {
      HGUtils.dropHyperGraphInstance(location)
    }
  }

  it should "allow data to be stored without transactions" in { (fixture: FixtureParam) =>  
    val location = freshDatabaseLocation()
    try {
      val config = new HGConfiguration()
      config.setEnforceTransactionsInStorageLayer(false)
      storeImplementation = Class.forName(storeImplementationClass).newInstance.asInstanceOf[HGStoreImplementation]
      config.setStoreImplementation(storeImplementation)
      (new java.io.File(location)).mkdirs()      
      val store = new HGStore(location, config)
      val handles = handleArray(4)
      val result = store.store(handles)
      store.getLink(result) should be(handles)
      store.close()
    }
    finally {
      HGUtils.dropHyperGraphInstance(location)
    }
  }

  it should "ignore stored data in main storage upon transaction rollback"   in { (fixture: FixtureParam) =>  
    // val somedata = randomBytes(100)    
    // store.getTransactionManager.beginTransaction()
    // var handle = store.store(somedata)
    // store.getData(handle) should be(somedata)
    // store.getTransactionManager.abort()
    // store.getData(handle) should be(null)

    val link = handleArray(2)
    store.getTransactionManager.beginTransaction()
    val handle = store.store(link)
    store.getLink(handle) should be(link)
    store.getTransactionManager.abort()
    store.getLink(handle) should be(null)
  }

  it should "ignore stored data in index upon transaction rollback"   in { (fixture: FixtureParam) =>  
    val idx = store.getIndex("III", 
          BAtoBA.getInstance(), 
          BAtoBA.getInstance(), 
          null, 
          null, 
          true)
    try {
      val key = randomBytes(5)
      val value = randomBytes(10)
      store.getTransactionManager.beginTransaction
      idx.addEntry(key, value)
      idx.findFirst(key) should be(value)
      store.getTransactionManager.abort
      idx.findFirst(key) should be(null)
    }
    finally {
      store.removeIndex("III")
    }
  }

  it should "ignore stored data in bi-directional index upon transaction rollback"   in { (fixture: FixtureParam) =>  
    val idx = store.getBidirectionalIndex("III", 
          BAtoBA.getInstance(), 
          BAtoBA.getInstance(), 
          null, 
          null, 
          true)
    try {
      val key = randomBytes(5)
      val value = randomBytes(10)
      store.getTransactionManager.beginTransaction
      idx.addEntry(key, value)
      idx.findFirst(key) should be(value)
      idx.findFirstByValue(value) should be(key)
      store.getTransactionManager.abort
      idx.findFirst(key) should be(null)
      idx.findFirstByValue(value) should be(null)
    }
    finally {
      store.removeIndex("III")
    }
  }
}
