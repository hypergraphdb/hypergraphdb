package hgtest.storage

import org.scalatest.FunSuite
import scala.Predef._
import org.hypergraphdb.{HGIndex, HGStore, HyperGraph, HGRandomAccessResult}

//import org.scalatest.Assertions.assert
import TestCommons._
import scala.collection.JavaConversions._

//to be tested: Interface HGIndex
//  addEntry,   removeEntry, removeAllEntries, findFirst, find,
//  open() {};  close() {};  isOpen = false;  scanKeys()
//  scanValues();  count(); count(key: KeyType)

// Test remaining to be written:
// HGSortIndex: HGSearchResult<ValueType> findLT; findGT; findLTE; findGTE;


class IndexTest2 extends FunSuite {
  var graph: HyperGraph = null // initializeGraph("localhost", "6378")
  def setGraph(graphi:HyperGraph) : IndexTest2  = { graph = graphi; this }
  var graphstore:HGStore = null // graph.getStore;
  var index:HGIndex[String, String] = null //graphstore.getIndex(randomString, baToString, baToString, baComp, true)
  val dataMap2: StringListMap = genStriLiMap()

  def genStriLiMap(accuMap: Map[String, List[String]] = Map.empty[String, List[String]],
                   length: Int = dataSize): Map[String, List[String]] = {
    if (length > 0)
      genStriLiMap(accuMap + (randomString -> randomStringList()), length - 1)
    else
      accuMap
  }


  def init() = {
    graphstore = graph.getStore
    index = graphstore.getIndex("notArandomString", baToString, baToString, baComp, true)

  }

  test("Adding StringKey-StringTuple pairs to Index") {
    init()
    assert(dataMap2.keySet.size > 10)
    assert(dataMap2.values.size > 10)
    dataMap2.foreach {case (k: String, v: List[String]) => v.foreach(s => index.addEntry(k, s))  }
    Thread.sleep(200)
  }

  test("checking scanValues: All values of dataMap are present in index?") {
    val flattenDataMap2Values = dataMap2.values.flatten.toSet
    assert(index.scanValues().forall(s => flattenDataMap2Values contains s))
  }

  test("checking scanKeys: All keys of dataMap are present in index?") {
    val dataMapKeySet = dataMap2.keys.toSet
    assert(index.scanKeys forall (s => dataMapKeySet contains s))
  }

  test("checking findFirst: List of each key contains value of index.findFirst(k)?") {
    assert(dataMap2 forall {
      case (k: String, v: List[String]) => v contains (index.findFirst(k))
    })
  }

  test("checking find.next: dataMap2 forall { case (k: String, v: List[String]) => index.find(k).forall(s => v.contains(s) ? ") {
    //1    assert(dataMap2 forall { case (k: String, v: List[String]) => index.find(k).sameElements(v.iterator)})
    assert(dataMap2 forall { case (k: String, v: List[String]) => index.find(k).forall(s => v.contains(s))})
  }

  test("checking Two-Way-Iterator funtions of HGRandomAccessResult") {
    dataMap2.foreach{case (k: String, v: List[String]) => test2WayIterator(index.find(k))}
  }

  test("removing the first of each List, then checking if they are all absent afterwards") {
    println("index.count() before removing anything: " + index.count())
    dataMap2 foreach {
      case (k: String, v: List[String]) => index.removeEntry(k, v.head)
    }
    Thread.sleep(200)
    assert(dataMap2 forall {
      case (k: String, v: List[String]) => index.find(k).goTo(v.head, true).equals(HGRandomAccessResult.GotoResult.nothing)
    })
  }

  test("checking counts. equals dataMap.keys.size?") {
    println("index.count(): " + index.count())
    println(dataMap2.keys.size)
    assert(index.count() == (dataMap2.keys.size))
  }

  test("checking removingAll. ") {
    dataMap2.keys.foreach(k => index.removeAllEntries(k))
    assert(dataMap2.keys.forall {
      k => index.findFirst(k) == null
    })

  }
}