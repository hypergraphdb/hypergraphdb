package hgtest.storage

import TestCommons._
import org.hypergraphdb.handle.UUIDHandleFactory;
import org.hypergraphdb.storage.{BAtoString, ByteArrayConverter, HGStoreImplementation}
import java.util.{Arrays, Comparator}
import org.hypergraphdb._

import collection.JavaConversions._
import collection.mutable.HashMap
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import org.scalatest.FunSuite


class TestStoreImpl extends  FunSuite{
  
  val sbaconvert = new BAtoString
  val hanGen = new UUIDHandleFactory
  var graph: HyperGraph = null //TestCommons.initializeGraph("/home/ingvar/workspace/intellij/jedisHGDB", "6378")
  var store: HGStore = null
//  val handleGen = new UUIDHandleFactory;
//  val stringHandleTupleSet = byteASet.map(ba => (handleGen.makeHandle, ba))
  val syncTime = 100

  //
  // test methods of dataDB: store, removeData, getData
  //def testDataStorage(dataSetSize:Int):Boolean = {
  def setGraph(graphi:HyperGraph) : TestStoreImpl  = { graph = graphi; this }
  def init() = { store = graph.getStore }

  test("testing DataDB funtions"){
    init()

    val hashToByteArrayMap = HashMap.empty[Int, Array[Byte]]
    TestCommons.randomStringList(length = dataSize).foreach( s => hashToByteArrayMap += (Arrays.hashCode(sbaconvert.toByteArray(s)) -> sbaconvert.toByteArray(s)))

    //def testStoreAndGet
    val hashToHandleMap = hashToByteArrayMap.map{case (hash, byteArray) => (hash -> store.store(byteArray))}
    Thread.sleep(syncTime) // give some sync time
    val baBack = hashToHandleMap.map{case(hash, handle) => store.getData(handle).asInstanceOf[Array[Byte]]}
    //assert everything has been successfully stored.
    assert(baBack.forall{ba:Array[Byte] => hashToByteArrayMap.isDefinedAt(Arrays.hashCode(ba))})

    //some entries to be removed
    val toBeDeletedEntries = hashToByteArrayMap.drop(hashToByteArrayMap.size/2).take(hashToByteArrayMap.size/3)
    // remove those and let it sync
    toBeDeletedEntries.foreach{case (hash, handle) => store.removeData(hashToHandleMap(hash))}
    Thread.sleep(syncTime)
    //confirm each one is indeed removed.
    assert(toBeDeletedEntries.forall{case (hash, handle) => (try{store.getData(hashToHandleMap(hash)) == null} ) })
    
    // check the remaining ones are still present    
    val newEntrySet = hashToByteArrayMap.entrySet().diff(toBeDeletedEntries.entrySet())   // hmm, don't know how to do it without Java.Conversions
    newEntrySet.forall{ case e:java.util.Map.Entry[Int,Array[Byte]] => Arrays.equals(store.getData(hashToHandleMap(e.getKey)),e.getValue) }
  }

  //
  // test methods of linkDB: store, getLink, removeLink, containsLink
  //
  //def testLinkStorage(dataSetSize:Int): Boolean = {
    test("testing LinkDB-Functions"){
    val phAr:Array[HGPersistentHandle] = Array(hanGen.makeHandle(),hanGen.makeHandle(),hanGen.makeHandle())
    val phArBack = store.getLink(store.store(phAr))

    val hash2HandleListMap:Map[Int, List[HGPersistentHandle]] = genHashToHHListMap(length = dataSize)
    val hashTohandleMap:Map[Int, HGPersistentHandle]= hash2HandleListMap.map{ case(hash, handleList) => (hash -> store.store(handleList.toArray[HGPersistentHandle]))}
    assert(hashTohandleMap.forall{ case(hash, handle) => store.containsLink(handle) == true})  // why does this fail?
    val failVsSucceed = hashTohandleMap.groupBy{ case(hash, handle) => store.containsLink(handle) == true }
    //assert(failVsSucceed.XY.false.size == 0)
    assert(hashTohandleMap.forall{ case(hash, handle)  => hashTravHPH(store.getLink(handle)).equals(hash) })

    
    val toBeDeletedEntries = hash2HandleListMap.drop(hash2HandleListMap.size/2).take(hash2HandleListMap.size/3)
    toBeDeletedEntries.foreach{ case(hash, handleList) => store.removeLink(hashTohandleMap(hash))}
    Thread.sleep(syncTime)
    assert(toBeDeletedEntries.forall{ case(hash, hL) =>  store.getLink(hashTohandleMap(hash)) == null})
    assert(toBeDeletedEntries.forall{ case(hash, hL) =>  store.containsLink(hashTohandleMap(hash)) == false})

    val notDeleted = hash2HandleListMap.take(hash2HandleListMap.size/2).toSet.union(hash2HandleListMap.drop(hash2HandleListMap.size/2).drop(hash2HandleListMap.size/3 +1).toSet)               //adding -1
    assert(notDeleted.forall{ case(hash, hL) =>  store.containsLink(hashTohandleMap(hash)) == true})
    notDeleted.forall{ case(hash, hL) =>  hashTravHPH(store.getLink(hashTohandleMap(hash))).equals(hash)}

  }

//
//Incidence Link related:
//
  //def getIncidenceResultSet(handle: HGPersistentHandle) = null
  //def removeIncidenceSet(handle: HGPersistentHandle) {}
  //def getIncidenceSetCardinality(handle: HGPersistentHandle) = 0L
  //def addIncidenceLink(handle: HGPersistentHandle, newLink: HGPersistentHandle) {}
  //def removeIncidenceLink(handle: HGPersistentHandle, oldLink: HGPersistentHandle) {}



  //def testIncidenceLinkStorage(dataSetSize:Int):Unit = {
    test("testing IncidenceDB-functions"){
    //trivial:
    val handle = hanGen.makeHandle();
    val linkSet = Stream.fill[HGPersistentHandle](dataSize)(hanGen.makeHandle()).toSet
    linkSet.foreach(link => store.addIncidenceLink(handle, link))
    val incReSet = store.getIncidenceResultSet(handle)
    assert(!(try { incReSet.hasPrev } catch { case  t:Throwable => false}))
    test2WayIterator(incReSet)

    incReSet.goBeforeFirst();
    while (incReSet.hasNext)
    {
      assert(linkSet.contains(incReSet.next))
    }
    val cardinality= store.getIncidenceSetCardinality(handle)

    assert(cardinality == dataSize)

    val toBeDeletedOrNot = linkSet.groupBy(hph => (hph.hashCode() > 0))
    toBeDeletedOrNot(true).foreach(hph => store.removeIncidenceLink(handle, hph))
    assert(toBeDeletedOrNot(true).forall(hph => store.getIncidenceResultSet(handle).goTo(hph, true).equals(GotoResult.nothing)))  //GotoResult.nothing: AssertionFailre
    assert(toBeDeletedOrNot(false).forall(hph => store.getIncidenceResultSet(handle).goTo(hph, true).equals(GotoResult.found)))

    store.removeIncidenceSet(handle)
    assert(!store.getIncidenceResultSet(handle).hasNext)

    
  }

  //Index-related
  //def getIndex[KeyType, ValueType](name: String, keyConverter: ByteArrayConverter[KeyType], valueConverter: ByteArrayConverter[ValueType], comparator: Comparator[_], isBidirectional: Boolean, createIfNecessary: Boolean) = null
  //def removeIndex(name: String) {}

  //def testIndexFunctions(dataSetSize:Int):Unit = {
  test("testing Index-functions"){
    val striLi = randomStringList(length = 10)
    striLi.foreach{s => store.getIndex(s, baToString, baToString, baComp, true)}
    val toBeDeletedOrNot = striLi.groupBy(s => s.hashCode() > 0)
    toBeDeletedOrNot(true).foreach(s => store.removeIndex(s))
    assert(toBeDeletedOrNot(true).forall(s => store.getIndex(s, baToString, baToString, baComp, false) == null))

  }


  
  //Utility Methods

  def genHashToHHListMap(accuMap: Map[Int, List[HGPersistentHandle]] = Map.empty[Int, List[HGPersistentHandle]],
                   length: Int = dataSize): Map[Int,List[HGPersistentHandle]] = {
    if (length > 0){
      val hali = (1 to random.nextInt (50)).map(i => hanGen.makeHandle).toList;
      genHashToHHListMap(accuMap + (hashTravHPH(hali) -> hali), length - 1)
    }
    else
      accuMap
  }

  def hashTravHPH(hphTrav:Traversable[HGPersistentHandle]):Int = hphTrav.map(b => b.hashCode()).foldLeft(1)(_+_/ hphTrav.size)

}
