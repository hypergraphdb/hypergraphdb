package org.hypergraphdb.storage.hazelstore.testing

import scala.Predef._
import org.hypergraphdb._
import com.hazelcast.core.Hazelcast
import org.hypergraphdb.storage.hazelstore.{Hazelstore4, Hazelstore3, HazelStoreConfig}

import TestCommons._
import scala.collection.JavaConversions._
import scala.Some


object IndexTest {
    val hs = new Hazelstore4
    val config = new HGConfiguration
    config.setTransactional(false)
    config.setStoreImplementation(hs)
    val store:HGStore = new HGStore("bla", config)//getStore()
    val index:HGIndex[String, String] = store.getIndex(randomString, baToString, baToString, baComp, true)
    //val index:HGIndex[String, String] = store.getBidirectionalIndex(randomString, baToString, baToString, baComp, true)
    val dataMap2: StringListMap = genStriLiMap()

    def genStriLiMap(accuMap: Map[String, List[String]] = Map.empty[String, List[String]],
                     length: Int = dataSize): Map[String, List[String]] = {
      if (length > 0)
        genStriLiMap(accuMap + (randomString -> randomStringList()), length - 1)
      else
        accuMap
    }


    def init() {
    /*  graphstore = getStore()
      val config:HGConfiguration = new HGConfiguration
      // val handleFactory: SequentialUUIDHandleFactory = new SequentialUUIDHandleFactory(System.currentTimeMillis, 0)     ; config.setHandleFactory(handleFactory); config.setUseSystemAtomAttributes(false)
      config.setStoreImplementation(hs)
      graphstore = new HGStore("bla", config)
      hs.startup(graphstore,config)

      index = graphstore.getIndex(randomString, baToString, baToString, baComp, true)
      */
    }

    def main(args: Array[String]) {

      init()
      dataMap2.foreach {case (k: String, v: List[String]) => v.foreach(s => index.addEntry(k, s))  }
      val count = index.count
      assert(count == dataMap2.size)
      val dataMapValueSet = dataMap2.values.flatten.toSet
      //val flattenDataMap2ValueSet2 = dataMap2.foldLeft(Set.empty[String]){case (set, (string, stringlist)) => set ++ stringlist.toSet}
      val scanValues = index.scanValues
      val scanValueSet = scanValues.toSet
      assert(dataMapValueSet.size > dataSize)
      //assert(scanValueSet.intersect(flattenDataMap2Values).size.equals(flattenDataMap2Values.size))
      assert(scanValueSet.size.equals(dataMapValueSet.size))
      assert(scanValueSet.contains(dataMapValueSet.head))

      assert(dataMapValueSet.forall{string => scanValues.goTo(string, true).equals(HGRandomAccessResult.GotoResult.found)})

      val dataMapKeySet = dataMap2.keys.toSet
      assert(dataMapKeySet.size > dataSize/2)                             // since it's a set, it may be a little bit smaller than dataMap2.keys
      val scanKeys = index.scanKeys()
      assert(scanKeys.size >= dataSize -2 )
      assert(scanKeys forall (s => dataMapKeySet contains s))

      assert(index.count() > 0)

      assert(dataMap2 forall {
        case (k: String, v: List[String]) => v contains (index.findFirst(k))
      })

      val striStriSetMap = dataMap2 map {
        case (k: String, v: List[String]) => (k, v.toSet)
      }

      val example1stKVals = dataMap2.head._2.toSeq
      val dmValCountMap = example1stKVals.foldLeft(Map.empty[String, Int])((s,t) => s + (t -> example1stKVals.filter(u => u equals t).size))
      val exampleFindK = index.find(dataMap2.head._1).toSeq
      val indexValCountMap = exampleFindK.foldLeft(Map.empty[String, Int])((s,t) => s + (t -> exampleFindK.count(u => u equals t)))                            //(s => (s, exampleFindK.count(t => t equals s)))

      assert(dmValCountMap.forall{ case (a,b) => indexValCountMap(a) == b})

      // Todo -- we cannot be entirely sure that dataMap2 stringlists values are sets, i.e. that there are no dups. Correct & Complete this test.
      /*    val indexValSetElemCountMap = striStriSetMap.foldLeft(Map.empty[String, Map[String, Int]]){ case (k: String, v: Set[String]) => { val findK = index.find(k).toSeq; (k, v.map(s1 => (s1, findK.count(s2 => (s1 equals s2)))))}}
          // val dataMapValSetElemCountMap = striStriSetMap.map{ case (k: String, v: Set[String]) => { val findK = dataMap2(k); (k, v.map(s1 => (s1, findK.count(s2 => (s1 equals s2)))))}}
          val dataMapValSetElemCountMap = striStriSetMap.map{ case (k: String, v: Set[String]) => { val findK = dataMap2(k); (k, v.map(s1 => (s1, findK.count(s2 => (s1 equals s2)))))}}
          val equals3 = dataMapValSetElemCountMap ==  indexValSetElemCountMap
        */

      //  set based
      val notDif = striStriSetMap.forall{ case (k: String, v: Set[String]) => v.diff(index.find(k).toSet).size == 0 }
      val intersect_size_equals= striStriSetMap.forall{ case (k: String, v: Set[String]) => v.intersect(index.find(k).toSet).size == v.size   }
      assert(striStriSetMap.size >= dataSize-2)
      assert(notDif)   // ToDO - failed on 2.feb 13

      assert(intersect_size_equals)


      assert(index.count >0)
      dataMap2.foreach{case (k: String, v: List[String]) => test2WayIterator(index.find(k))}

      assert(index.count >0)
      val indexKeyValueCountMap = dataMap2 map { case (k: String, v: List[String]) => (k, index.count(k))}
      val allPresentBefore = dataMap2 forall { case (k: String, v: List[String]) => { val findK = index.find(k).toSeq; v.forall(s => findK.find(s => s == v.head).isDefined) }}

      dataMap2 foreach { case (k: String, v: List[String]) => index.removeEntry(k, v.head)}


      val indexKeyValueCountMapBack = dataMap2 map { case (k: String, v: List[String]) => (k, index.count(k))}
      val allPresentAfter = dataMap2 forall { case (k: String, v: List[String]) => { val findK = index.find(k).toSeq; v.forall(s => findK.find(s => s == v.head).isDefined) }}

      val allOneRemoved = indexKeyValueCountMap.forall{ case (k: String, c:Long) => (c - index.count(k) == 1)}
      assert(allOneRemoved)
      val notRemoved = dataMap2.filter{ case (k: String, v: List[String]) => index.find(k).contains(v.head) }
      assert(dataMap2 forall { case (k: String, v: List[String]) => index.find(k).goTo(v.head, true).equals(HGRandomAccessResult.GotoResult.nothing)     })

      println("index.count(): " + index.count())
      println(dataMap2.keys.size)
      assert(index.count() == dataMap2.keys.size)

      dataMap2.keys.foreach(k => index.removeAllEntries(k))
      Thread.sleep(2000)
      val findAfterRemove = dataMap2.keys.map {        k => index.findFirst(k)  }

      assert(findAfterRemove.forall(_ == null))

      println("finished test!")

      hs.shutdown()

    }

}
