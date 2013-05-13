package org.hypergraphdb.storage.hazelstore

import collection.JavaConversions._
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._
import org.hypergraphdb.{HGIndex, HGRandomAccessResult}
import com.hazelcast.core.Hazelcast
import org.hypergraphdb.util.CountMe
import org.hypergraphdb.storage.hazelstore.testing.Generators


class IndexTest(val index:HGIndex[String,String], async:Boolean)(implicit testDataSize: Int){

  def run:Long = {
    val a = timeMeasure(test)._1
    if (async)
      (a/1000 - 3*syncTime*1000)
    else
      a/1000
  }

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
    assert(scanVals._3)
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
          val a = index.find(key).asInstanceOf[HazelRS3[String]]
          assert(a.count == stringSet.size.toLong)
          a.forall(s => stringSet.contains(s))
    }})


    // TESTING findFirst(key: KeyType)
    allValSet.forall{case (key,stringSet) => stringSet.contains(index.findFirst(key))}


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
      a.forall(s => stringSet.contains(s))
    }})

    // TESTING removeAllEntries(key: KeyType) {}
    keySet.foreach(key => index.removeAllEntries(key))

    assert(keySet.forall(key => { repeatUntil(index.find,key)(_.asInstanceOf[CountMe].count == 0)._3}))
    val oneKeyLess = index.scanKeys().asInstanceOf[CountMe].count
    assert(oneKeyLess == 0)

    Hazelcast.shutdownAll()
  }

}
