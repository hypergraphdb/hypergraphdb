package org.hypergraphdb.storage.hazelstore.testing

import collection.JavaConversions._
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._
import org.hypergraphdb.storage.hazelstore.{HazelBidirecIndex, HazelIndex, HazelRS3}
import org.hypergraphdb.{HGSearchResult, HGSortIndex, HGIndex, HGRandomAccessResult}
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import com.hazelcast.core.Hazelcast
import org.hypergraphdb.util.CountMe


class IndexTest(val index:HGSortIndex[String,String], async:Boolean)(implicit testDataSize: Int){

  def run:Long = timeMeasure(test)._1

  def test{
    import Generators.Strings._
    val dataMap: Map[String, List[String]] = genStriLiMap()
    val vals = dataMap.map {case (key,stringList) => stringList}.flatten
    val valSet = vals.toSet

    //TESTING addEntry
    dataMap.foreach{case (key,stringList) => { stringList.foreach(index.addEntry(key,_))}}

    // TESTING scanValues()
    //val scanVals = repeatUntil1(() => index.scanValues)((x:HGRandomAccessResult[String]) => valSet.diff(x.asInstanceOf[HazelRS3[_].count == valSet.size.toLong).size == 0)
    val scanVals = repeatUntil1(() => index.scanValues)((x:HGRandomAccessResult[String]) => x.asInstanceOf[CountMe].count == valSet.size.toLong)
    assert(repeatUntil1(() => index.scanValues)((x:HGRandomAccessResult[String]) => x.asInstanceOf[CountMe].count == valSet.size.toLong)._3)
    assert(scanVals._1.size == valSet.size)

    // TESTING  scanKeys()
    val scanKeys = index.scanKeys()
    val keySet = dataMap.map(_._1).toSet
    assert(scanKeys.forall(keySet.contains(_)))


   // TESTING  count(key: KeyType)
    val a = dataMap.forall{case (key,stringList) => {
      val a = index.count(key)
      a == stringList.size.toLong
    }}
    if(async && !a)
      println("\n\n\nINDEX: count(key) failed!")
    else
      assert(a)

    // TESTING count()
    val dataMapSize = dataMap.size.toLong
    val indexKeyCount = index.count
    if(async && indexKeyCount != dataMapSize)
      println("\n\n\nINDEX: indexKeyCount != dataMapSize")
    else
      assert(indexKeyCount == dataMapSize)


    // TESTING find(key: KeyType)
    val allValSet = dataMap.map{case (key,stringList) => (key,stringList.toSet)}
    assert(allValSet.forall{ case (key,stringSet) => {
          val a = index.find(key)//.asInstanceOf[HazelRS3[String]]
          assert(a.asInstanceOf[CountMe].count == stringSet.size.toLong)
          a.forall(s => stringSet.contains(s))
    }})


    // TESTING findFirst(key: KeyType)
    allValSet.forall{case (key,stringSet) => stringSet.contains(index.findFirst(key))}

    // TESTING HGSortIndex functions
    //    def findLT(key: KeyType): HGSearchResult[ValueType]
    //    def findGT(key: KeyType): HGSearchResult[ValueType]
    //    def findLTE(key: KeyType): HGSearchResult[ValueType]
    //    def findGTE(key: KeyType): HGSearchResult[ValueType]

    //val bacomp =  if(index.isInstanceOf[HazelIndex[_,_]]) index.asInstanceOf[HazelIndex[_,_]].comparator else index.asInstanceOf[HazelBidirecIndex[_,_]].comparator

    val stringOrder = new Ordering[String]{
     def compare(x: String, y: String): Int = baComp.compare(x.getBytes,y.getBytes)
   }

    def findTests(testDataFun: (String,String)=> Boolean, indexFun : (HGSortIndex[String,String],String) => HGSearchResult[String]):Boolean =
    {
        val elemToCompareWith = allValSet.drop(allValSet.size/2).head._1
        val dataFiltered        = allValSet.filter( pair => testDataFun(pair._1,elemToCompareWith)).map(pair => pair._2).flatten
        val indexFiltered     = indexFun(index, elemToCompareWith)
        dataFiltered.size == indexFiltered.asInstanceOf[CountMe].count && dataFiltered.toSet.intersect(indexFiltered.toSet).size == dataFiltered.size
    }

    assert(findTests(stringOrder.lt(_,_),_.findLT(_)))
    assert(findTests(stringOrder.lteq(_,_),_.findLTE(_)))
    assert(findTests(stringOrder.gt(_,_),_.findGT(_)))
    assert(findTests(stringOrder.gteq(_,_),_.findGTE(_)))


    // TESTING removeEntry(key: KeyType, value: ValueType)
    val toBeRemoved = dataMap.drop(testDataSize/4).take(testDataSize/3).map{ case (key,stringList) => (key, stringList.take(stringListLenght/2).toSet)}
   toBeRemoved.foreach{ case (key,stringSet) => stringSet.foreach(i => index.removeEntry(key,i)) }

    val notContained = toBeRemoved.map {
      case (key, stringSet) => {
        repeatUntil(index.find,key)(_.forall(s => !stringSet.contains(s)))
      }
    }
    assert(notContained.forall(_._3 == true))

    // still finding the old data?
    val newDataMap = allValSet.map   {case (key,stringSet) => (key -> stringSet.diff(toBeRemoved.getOrElse(key,Set())))}
    assert(newDataMap.forall{case (key,stringSet) => {
      val a = index.find(key)
      assert(a.asInstanceOf[CountMe].count == stringSet.size.toLong)
      a.forall(s => stringSet.contains(s))    // this boolean is asserted by being wrapped in an assert 3 lines above...
    }})


    // TESTING removeAllEntries(key: KeyType) {}
    keySet.foreach(key => index.removeAllEntries(key))

    assert(keySet.forall(key => { repeatUntil(index.find,key)(_.asInstanceOf[CountMe].count == 0)._3}))
    val oneKeyLess = index.scanKeys().asInstanceOf[CountMe].count
    assert(oneKeyLess == 0)

    Hazelcast.shutdownAll()
  }

}
