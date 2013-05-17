package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb._
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._
import org.hypergraphdb.handle.UUIDHandleFactory
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import org.hypergraphdb.storage.hazelstore.EmptySR
import com.hazelcast.core.Hazelcast
import collection.JavaConversions._

class StorageTest (val store:HGStore, async:Boolean)(implicit testDataSize:Int ) {
  val handleFactory:HGHandleFactory = new UUIDHandleFactory

  def run:Long = {
    timeMeasure(test)._1
  }

  def test():Long =
  {
    val res = timeMeasure[Unit](
        {
          testIncidenceSet
          testDataStoreContainsGetRemoveGet
          testLinkStoreContainsGetRemoveGet
          testIndexGetRemove
        }
    )._1
  Hazelcast.shutdownAll
    res
  }

  def testDataStoreContainsGetRemoveGet {

  /*
  def store(handle: HGPersistentHandle, data: Array[Byte]) = ???
  def getData(handle: HGPersistentHandle) = ???
  def removeData(handle: HGPersistentHandle) {}
  def containsData(handle: HGPersistentHandle) = ???
*/
    println("\n\n S T A R T I N G   testDataStoreContainsGetRemoveGet\n\n")
    val testStrings = (1 to testDataSize).map(random.nextString(_).getBytes)
    val testStringPlusHandle = testStrings.map(ba => (ba,store.store(ba)))
    assert(testStringPlusHandle.forall(pair => repeatUntil(store.containsData,pair._2)(_ == true)._3))
    assert(testStringPlusHandle.forall(pair => repeatUntil(store.getData,pair._2)(arraysEqual(_,pair._1))._3))

    val toBeRemoved = testStringPlusHandle.drop(testDataSize/4).take(testDataSize/3)
    toBeRemoved.foreach(pair => store.removeData(pair._2))
    assert(toBeRemoved.forall(pair => repeatUntil(store.getData,pair._2)(_ == null)._3))
    val notRemoved = testStringPlusHandle.diff(toBeRemoved)
    assert(notRemoved.forall(pair => store.containsData(pair._2)))
    assert(notRemoved.forall(pair => arraysEqual(store.getData(pair._2),pair._1)))
    println("\n\n F I N I S H E D  testDataStoreContainsGetRemoveGet\n\n")
  }

 def testLinkStoreContainsGetRemoveGet {
   /*
     def store(handle: HGPersistentHandle, link: Array[HGPersistentHandle]) = ???
     def getLink(handle: HGPersistentHandle) = ???
     def removeLink(handle: HGPersistentHandle) {}
     def containsLink(handle: HGPersistentHandle) = ???
     */

   println("\n\n S T A R T I N G testLinkStoreContainsGetRemoveGet\n\n")
     val links = (1 to testDataSize).map(i => (1 to i).map(i => handleFactory.makeHandle()).toArray)
     val linkPairedHandle = links.map(l => (l,store.store(handleFactory.makeHandle(),l)))
   //if(async) Thread.sleep
     assert(linkPairedHandle.forall(pair => repeatUntil(store.containsLink, pair._2)(_ == true)._3))
   val gotsLinks = linkPairedHandle.map(pair => (store.getLink(pair._2), pair._1))
   assert(gotsLinks.forall(pair => arraysEqual(pair._1,pair._2)))

    val toBeRemoved = linkPairedHandle.drop(testDataSize/4).take(testDataSize/3)
    toBeRemoved.foreach(pair => store.removeLink(pair._2))

    assert(toBeRemoved.forall(pair => repeatUntil(store.getLink,pair._2)(_ == null)._3))
    val notRemoved = linkPairedHandle.diff(toBeRemoved)
   assert(notRemoved.forall(pair => store.containsLink(pair._2)))
   assert(notRemoved.forall(pair => arraysEqual(store.getLink(pair._2),pair._1)))
   println("\n\n F I N I S H E D testLinkStoreContainsGetRemoveGet")
  }

  def testIncidenceSet{
    def assertAll(h:HGPersistentHandle,set:Set[HGPersistentHandle],rs:HGRandomAccessResult[HGPersistentHandle]):GotoResult => Unit =
      (x:GotoResult) => { assert(set.forall(i => rs.goTo(i, true).equals(x)))}

    /*
      def getIncidenceResultSet(handle: HGPersistentHandle) = ???
      def removeIncidenceSet(handle: HGPersistentHandle) {}
      def getIncidenceSetCardinality(handle: HGPersistentHandle) = ???
      def addIncidenceLink(handle: HGPersistentHandle, newLink: HGPersistentHandle) {}
      def removeIncidenceLink(handle: HGPersistentHandle, oldLink: HGPersistentHandle) {}
      */
  val h1 = handleFactory.makeHandle()
  val inciSet = (1 to testDataSize).map(i => handleFactory.makeHandle()).toSet
  inciSet.foreach(i => store.addIncidenceLink(h1,i))

  //if(async) Thread.sleep
  val isc = repeatUntil(store.getIncidenceSetCardinality, h1)(_ == testDataSize.toLong)
  assert(store.getIncidenceSetCardinality(h1) == testDataSize.toLong)
    println("getIncidenceSetCardinality passed. Required synctime:" + isc._2)
//  val gic = repeatUntil(store.getIncidenceResultSet, h1){(x:HGRandomAccessResult[HGPersistentHandle]) => { val toSet = x.toSet; assert(toSet.size  > 0); assert(inciSet.intersect(toSet).size == inciSet.size); toSet.diff(inciSet).size == 0}}
  val gic =store.getIncidenceResultSet(h1)
  assertAll(h1,inciSet,gic)(GotoResult.found)

  val toBeRemoved  = inciSet.drop(testDataSize/4).take(testDataSize/3)
  toBeRemoved.foreach(h => store.removeIncidenceLink(h1,h))
  //if(async) Thread.sleep
  //val gicR1 = store.getIncidenceResultSet(h1)
  val iSR1 = inciSet.diff(toBeRemoved)
  val gicR1 = repeatUntil((x:HGPersistentHandle) => store.getIncidenceResultSet(x).toSet,h1)(_.diff(iSR1).size == 0)
  assert(gicR1._3)
  val gicR12 = store.getIncidenceResultSet(h1)
  assertAll(h1,iSR1,gicR12)(GotoResult.found)

  assertAll(h1,toBeRemoved,gicR12)(GotoResult.nothing)
  assert(store.getIncidenceSetCardinality(h1) == iSR1.size)

    store.removeIncidenceSet(h1)
    //if(async) Thread.sleep
    val empty = repeatUntil(store.getIncidenceResultSet, h1)(_ == EmptySR)
    assert(empty._3)
    assert(store.getIncidenceResultSet(h1)== EmptySR)
    println("\n\nSucceeded Incidence Set Test")
  }

  def testIndexGetRemove{
    val dontCreateIt = "dontCreate"
    /*
        def getIndex[KeyType, ValueType](name: String, keyConverter: ByteArrayConverter[KeyType], valueConverter: ByteArrayConverter[ValueType], comparator: Comparator[_], isBidirectional: Boolean, createIfNecessary: Boolean) = ???
        def removeIndex(name: String) {}
        */
    val dontCreate = store.getIndex(dontCreateIt, null, null, null, false)
    assert(dontCreate == null)
    store.removeIndex(dontCreateIt)

    val createIt = "create"
    val create = store.getIndex(createIt, null, null, null, true)
    assert(create != null)

    store.removeIndex(createIt)
    val createBack = store.getIndex(createIt, null, null, null, false)
    assert(createBack == null)

  }


}
