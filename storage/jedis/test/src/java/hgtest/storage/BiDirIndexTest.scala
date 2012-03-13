package hgtest.storage

import org.hypergraphdb.HGRandomAccessResult.GotoResult
import collection.mutable.Map
import org.scalatest.FunSuite

import hgtest.storage.TestCommons._
import org.scalatest.Assertions._
import org.hypergraphdb.{HGBidirectionalIndex, HGStore, HyperGraph}


/*
HGBiDirIndex:  findByValue(value: ValueType); findFirstByValue(value: ValueType); countKeys(value: ValueType)
*/

class BiDirIndexTest extends FunSuite {
  val random = new scala.util.Random
  var graph: HyperGraph = null //initializeGraph("localhost", "6378")
  def setGraph(graphi:HyperGraph) : BiDirIndexTest  = { graph = graphi; this }
  var graphstore:HGStore = null
  var biDirIndex:HGBidirectionalIndex[String, String] = null
  val dupeList: List[String] = randomStringList(length = dataSize)
  val dupeMap: Map[String, Int] = Map.empty[String, Int]
  dupeList.map(s => dupeMap += (s -> 0))

  val biDirDataMap: Map[String, List[String]] = genStringListMapSomeWithDupes(length = dataSize)
  //  val dupeMap:Map[String,  Int] = Map.empty[String, Int]

  def init() = {
    graphstore = graph.getStore
    biDirIndex = graphstore.getBidirectionalIndex("NotrandomString", baToString, baToString, baComp, true)
  }

  test("Adding StringKey-StringTuple pairs to Index, with some duplicate values occuring across several keys") {
    init()

    biDirDataMap.foreach {
      case (k: String, v: List[String]) => v.foreach(s => biDirIndex.addEntry(k, s))
    }
    Thread.sleep(400) //give time to sync
  }

  test("Checking findByValue(exactValue).goto(key, exact=true)") {
    //assert(biDirDataMap.keySet.size = 10)
    //assert(biDirDataMap.head._2.size  = 10)
    //assert(biDirDataMap.values.toSet.size = 10)
    biDirDataMap.forall { case (k: String, v: List[String]) => v.filter(randomFilter(_)(random)).forall(biDirIndex.findByValue(_).goTo(k, true).equals(GotoResult.found))}
  }

/*  test("Checking findByValue(value + emtySpace).goto(key, exact=false)") {
    //assert(biDirDataMap.forall {  case (k: String, v: List[String]) => v.filter(randomFilter(_)(random)).forall(s => biDirIndex.findByValue(s + " ").goTo(k, true).equals(GotoResult.close)) })
    assert(biDirDataMap.forall {  case (k: String, v: List[String]) => v.forall(s => biDirIndex.findByValue(s + " ").goTo(k, false).equals(GotoResult.close)) })
  }
*/

  test("performing Two-Way-Iterator test on findByValue"){
    biDirDataMap.foreach {  case (k: String, v: List[String]) => v.foreach(s => test2WayIterator(biDirIndex.findByValue(s)))}
  }
  test("Checking findFirstByValue") {
    biDirDataMap.forall {
      case (k: String, v: List[String]) => v.forall(s => biDirIndex.findFirstByValue(s).equals(k))
    }
  }

  test("Checking countKeys") {
    biDirDataMap.forall {
      case (k: String, v: List[String]) => v.forall(s => biDirIndex.countKeys(s).equals(dupeMap.getOrElse(s, 1)))
    }
  }

  def genStringListMapSomeWithDupes(accuMap: Map[String, List[String]] = Map.empty[String, List[String]],
                                    length: Int = 40): Map[String, List[String]] = {
    if (length > 0)
      if (random.nextBoolean()) {//randomly insert some duplicates {
        //          genStringListMapSomeWithDupes(accuMap + (randomString -> (dupeList.drop(random.nextInt(dupeList.size-2)) ::: randomStringList())), length - 1)
        val rand = random.nextInt(dupeList.size - 2)
        dupeMap.drop(rand).foreach {
          case (k: String, v: Int) => dupeMap.+(k -> (v + 1))
        }
        val tempMap = dupeMap.drop(rand)
        val tempList = tempMap.keys.toList
        genStringListMapSomeWithDupes(accuMap + (randomString -> (tempList ::: randomStringList())), length - 1)}

      else
        genStringListMapSomeWithDupes(accuMap + (randomString -> randomStringList()), length - 1)
    else
      accuMap
  }


  /*
  def returnDupesAndCountUse:List[String] = incrementValOnMutableMapReturnKeysAsList(dupeMap.drop(random.nextInt(dupeMap.size)))
  //dupeMap.drop(random.nextInt(dupeMap.size)).foreach(case (k: String, v: Int) => dupeMap)

  def incrementValOnMutableMapReturnKeysAsList[A,Int](map:java.util.Map[A,Int], f: Map[A,Int] => Unit):List[A] = {

    map.foreach( case (k: String, v: Int) => map.set(k, v))
    map.keys.toList
  }
*/
  def randomFilter[A](a: A)(implicit random: scala.util.Random): Boolean = random.nextBoolean()


}


/*
//        genStringListMapSomeWithDupes(accuMap + (randomString -> (returnDupesAndCountUse ::: randomStringList())), length - 1)
//        genStringListMapSomeWithDupes(accuMap + (randomString -> (dupeMap.drop(random.nextInt(dupeMap.size)) ::: randomStringList())), length - 1)
*/