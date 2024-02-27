package storage

import org.scalatest.funsuite.AnyFunSuite
import org.hypergraphdb._
import org.hypergraphdb.storage.StorageGraph
import org.hypergraphdb.storage.RAMStorageGraph
import org.hypergraphdb.storage.BAtoBA
import scala.util.Using
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import org.scalatest.Tag

class StorageImplementationTests extends StorageTestBase {
  
  val store: HGStore = getStore()

  // test 1
  it should "'double' should handle 0" in { 
    val h = store.getConfiguration().getHandleFactory().makeHandle()
    info("Handle = " + h)
    assert(h != null)
  }

  "containsData" should "return false on non-existing data" in {
    tx(assert(!store.containsData(newhandle())))
  }

  "containsData" should "return true on existing data" in {
    val h = newhandle()
    tx({ 
      store.store(h, randomBytes(10))
      assert(store.containsData(h)) 
    })
  }  

  "containsLink" should "return false on non-existing data" in {
    tx(assert(!store.containsLink(newhandle())))
  }

  "containsLink" should "return true on existing data" in {
    val h = newhandle()
    tx({
      store.store(h, handleArray(4))
      assert(store.containsLink(h))
    })
  }

  "addIncidentLink" should "work on an empty incidence set" in {
    val a = newhandle()
    val incident = newhandle()
    tx(store.addIncidenceLink(a, incident))
    tx(assert(store.getIncidenceSetCardinality(a) == 1))
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(incident.equals(rs.next)); assert(!rs.hasNext) })
  }

  "addIncidentLink" should "work on a non-empty incidence set" in {
    val a = newhandle()
    val incident = newhandle()    
    tx({
      store.addIncidenceLink(a, newhandle())
      store.addIncidenceLink(a, newhandle())
      store.addIncidenceLink(a, newhandle())
      store.addIncidenceLink(a, newhandle())
      store.addIncidenceLink(a, incident)
    })
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(rs.goTo(incident, true) == GotoResult.found) } )
  }

  "addIncidentLink" should "work when adding a large number of incident links, many small transactions" in {
    val max = 1000000
    val incident = scala.collection.mutable.Set[HGHandle]()
    val a = newhandle()
    for (i <- 0 to max) {
      val link = newhandle()
      incident += link
      tx(store.addIncidenceLink(a, link))
    }
    incident.map(_.getPersistent).foreach(link => 
      tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(rs.goTo(link, true) == GotoResult.found)}))
  }

  "addIncidentLink" should "work when adding a large number of incident links, 1 transaction" in {
    val max = 100000
    val incident = scala.collection.mutable.Set[HGHandle]()
    val a = newhandle()
    tx(for (i <- 0 to max) {
        val link = newhandle()
        incident += link
        store.addIncidenceLink(a, link)
      },
      Some(() => incident.clear())
    )
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => 
      incident.map(_.getPersistent).foreach(link =>  assert(rs.goTo(link, true) == GotoResult.found))
     })
  }

  "addIncidentLink" should "ignore adding the same incident twice - no duplicates!" in {
    val a = newhandle()
    val i1 = newhandle()
    val i2 = newhandle()
    val i3 = newhandle()
    tx(store.addIncidenceLink(a, i1))
    tx(store.addIncidenceLink(a, i2))
    tx(store.addIncidenceLink(a, i3))
    tx(store.addIncidenceLink(a, i2))
    tx(assert(store.getIncidenceSetCardinality(a) == 3))
  }

  "addIncidentLink" should "throw an exception" in {
    assertThrows[Exception] { store.addIncidenceLink(newhandle(), null) }
    assertThrows[Exception] { store.addIncidenceLink(null, newhandle()) }
  }

  "addIncidentLink" should "work on the null handle as well" in {
    val nullhandle = store.getConfiguration.getHandleFactory.nullHandle
    val a = newhandle()
    tx(store.addIncidenceLink(a, nullhandle))
    tx(store.addIncidenceLink(nullhandle, a))
    tx(assertResultContains(store.getIncidenceResultSet(a), nullhandle))
    tx(assertResultContains(store.getIncidenceResultSet(nullhandle), a))
  }

  "removeIncidentLink" should "work with a null handles" in {
    val nullhandle = store.getConfiguration.getHandleFactory.nullHandle
    val a = newhandle()
    tx(store.addIncidenceLink(a, nullhandle))
    tx(store.addIncidenceLink(nullhandle, a))
    tx({
      assertResultContains(store.getIncidenceResultSet(a), nullhandle)
      store.removeIncidenceLink(a, nullhandle)
    })
    tx({
      assertResultContains(store.getIncidenceResultSet(nullhandle), a)
      store.removeIncidenceLink(nullhandle, a)
    })
    tx(assertNotInResult(store.getIncidenceResultSet(a), nullhandle))
    tx(assertNotInResult(store.getIncidenceResultSet(nullhandle), a))
  }

  "removeIncidentLink" should "work on a non-empty incidence set" in {
    val a = newhandle()
    val i1 = newhandle()
    tx({
      store.addIncidenceLink(a, i1)
      for (i <- 0 to 20) 
        store.addIncidenceLink(a, newhandle())
    })
    tx(store.removeIncidenceLink(a, i1))
    tx(assertNotInResult(store.getIncidenceResultSet(a), i1))
  }

  "removeIncidentLink" should "work on a singleton incidence set " in {
    val a = newhandle()
    val i1 = newhandle()
    tx(store.addIncidenceLink(a, i1))
    tx(store.removeIncidenceLink(a, i1))
    tx(assertNotInResult(store.getIncidenceResultSet(a), i1))
  }

  "removeIncidentLink" should "work on a large incidence set, many small transactions" in {
    val max = 1000000
    val incident = scala.collection.mutable.Set[HGHandle]()
    val a = newhandle()
    for (i <- 0 to max) {
      val link = newhandle()
      incident += link
      tx(store.addIncidenceLink(a, link))
    }
    incident.map(_.getPersistent).foreach(link => 
      tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(rs.goTo(link, true) == GotoResult.found)}))

    incident.map(_.getPersistent).foreach(link => tx(store.removeIncidenceLink(a, link)))
    tx(assert(store.getIncidenceSetCardinality(a) == 0))
  }  

  "removeIncidentLink" should "work on a large incidence set, single transaction" in {
    val max = 100000
    val incident = scala.collection.mutable.Set[HGHandle]()
    val a = newhandle()
    tx(for (i <- 0 to max) {
        val link = newhandle()
        incident += link
        store.addIncidenceLink(a, link)
      },
      Some(() => incident.clear())
    )
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => 
      incident.map(_.getPersistent).foreach(link =>  assert(rs.goTo(link, true) == GotoResult.found))
     })    

    tx(incident.map(_.getPersistent).foreach(link => store.removeIncidenceLink(a, link)))
    tx(assert(store.getIncidenceSetCardinality(a) == 0))
  }    

  "removeIncidentSet" should "work on a singleton set"  in {
    val a = newhandle()
    val incident = newhandle()
    tx(store.addIncidenceLink(a, incident))
    tx(assert(store.getIncidenceSetCardinality(a) == 1))
    tx(store.removeIncidenceLink(a, incident))
    tx(assert(store.getIncidenceSetCardinality(a) == 0))
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(!rs.hasNext) })
  }      

  "removeIncidentSet" should "work on an empty set" in {
    val a = newhandle()
    val incident = newhandle()
    tx(assert(store.getIncidenceSetCardinality(a) == 0))
    tx(store.removeIncidenceLink(a, incident))
    tx(assert(store.getIncidenceSetCardinality(a) == 0))
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => assert(!rs.hasNext) })
  }        

  "removeIncidentSet" should "work on a large incidence set"  taggedAs(ToDebug) in {
    val max = 100000
    val incident = scala.collection.mutable.Set[HGHandle]()
    val a = newhandle()
    var toremove: HGPersistentHandle = null
    var tocheck: HGPersistentHandle = null
    tx(for (i <- 0 to max) {
        val link = newhandle()
        incident += link
        store.addIncidenceLink(a, link)
        if (i == 2345)
          toremove = link
        else if (i == 6543)
          tocheck = link
      },      
      Some(() => incident.clear())
    )
    tx(store.removeIncidenceLink(a, toremove))
    tx(Using.resource(store.getIncidenceResultSet(a)) { rs => {
      assert(rs.goTo(tocheck, true) == GotoResult.found)
      assert(rs.goTo(toremove, true) == GotoResult.nothing)
    }})
  }  

  it should "throw an exception if attempted usage after a close" in {
    tx(store.containsData(newhandle()))
    store.close()
    assertThrows[RuntimeException] { store.store(newhandle(), randomBytes(10)) }
    assertThrows[RuntimeException] { store.store(newhandle(), handleArray(2)) }
    assertThrows[RuntimeException] { store.store(randomBytes(5)) }
    assertThrows[RuntimeException] { store.store(handleArray(3)) }    
    assertThrows[RuntimeException] { store.containsData(newhandle()) }
    assertThrows[RuntimeException] { store.containsLink(newhandle()) }
    assertThrows[RuntimeException] { store.getData(newhandle()) }
    assertThrows[RuntimeException] { store.getLink(newhandle()) }
    assertThrows[RuntimeException] { store.getIncidenceResultSet(newhandle()) }
    assertThrows[RuntimeException] { store.getIncidenceSetCardinality(newhandle()) }
    assertThrows[RuntimeException] { store.addIncidenceLink(newhandle(), newhandle()) }
    assertThrows[RuntimeException] { store.removeData(newhandle()) }
    assertThrows[RuntimeException] { store.removeLink(newhandle()) }
    assertThrows[RuntimeException] { store.removeIncidenceLink(newhandle(), newhandle()) }
    assertThrows[RuntimeException] { store.removeIncidenceSet(newhandle()) }
    assertThrows[RuntimeException] { store.getIndex("index" ) }
    assertThrows[RuntimeException] { store.removeIndex("index" ) }
    assertThrows[RuntimeException] { store.getBidirectionalIndex("index", BAtoBA.getInstance, BAtoBA.getInstance, null, null, true) }
  }
}