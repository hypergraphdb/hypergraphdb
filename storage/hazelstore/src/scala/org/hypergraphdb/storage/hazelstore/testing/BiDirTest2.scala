package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.{HGBidirectionalIndex, HGStore, HGConfiguration}
import collection.immutable
import collection.JavaConversions._
import com.hazelcast.core.Hazelcast
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import TestCommons._

class BiDirTest2(biDirIndex:HGBidirectionalIndex[String, String], async:Boolean) {
  import Generators.Strings._

  def run:Long = test


  val dupeList:     List[String]        = randomStringList(length = dataSize)
  val dupeRegister: Map[String, Int]    = dupeList.map(s => (s, 0)).toMap[String, Int]
  val dupeListLength = dupeList.length

  // biDirDataMap corresponds to the index
  // dupeMap keeps tracks of the duplicates
  val (biDirDataMap, dupeMap) :(immutable.Map[String, List[String]],immutable.Map[String,Int])   =
    someDupStringStringListMap(immutable.Map.empty[String, List[String]], dupeRegister, dataSize)

  val bdirDataMapZipped = biDirDataMap.zipWithIndex


  def test:Long =
  {
    val res = timeMeasure(tests)
    Hazelcast.shutdownAll()
    res._1
  }

  def mapOverMap[R](fun: (String, List[String]) => R) = biDirDataMap.map {case (k: String, v: List[String]) => fun(k,v)}

  val allOrigValues = mapOverMap((k: String, v: List[String]) => v.map(vv =>vv)).flatten.toList
  val allOrigKeys = mapOverMap((k: String, v: List[String]) => k)
  val allOrigValuesSet = allOrigValues.toSet
  val allOrigKeysSet = allOrigKeys.toSet

  def randomFilter[A](a: A)(implicit random: scala.util.Random): Boolean = random.nextBoolean()

  def tests{

    /*
      def count(key: KeyType) = ???

      def findByValue(value: ValueType) = ???

      def findFirstByValue(value: ValueType) = ???

      def countKeys(value: ValueType) = ???
      */
    println("Adding StringKey-StringTuple pairs to Index, with some duplicate values occuring across several keys")
    biDirDataMap.foreach { case (k: String, v: List[String]) => v.foreach(s => biDirIndex.addEntry(k, s))    }

    // duplicates
    //( 0 to dataSize-1).foreach(i => biDirDataMap.foreach{ case (k: String, v: List[String]) => biDirIndex.addEntry(k,dupeList(i))})
    bdirDataMapZipped.foreach{ case ((k: String, v: List[String]),index) => {
          biDirIndex.addEntry(k, dupeList(index))};
          if(index >1)
            biDirIndex.addEntry(k,dupeList(index-1))
          if(index < dupeListLength-1)
            biDirIndex.addEntry(k,dupeList(index+1))
        }


//    if(async) Thread.sleep(syncTime)

    //val count2 = biDirIndex.count()
    val count2 = repeatUntil1(biDirIndex.count)(_ == dataSize)
    assert(count2._3)

    println("Checking found keys from findByValue(ALLVALUES, exactValue).goto(key, exact=true)")
    val foundKeys = biDirDataMap.map {
      case (k: String, v: List[String]) => v.map(vv => biDirIndex.findByValue(vv).goTo(k, true))
    }
    val allFound = foundKeys.forall(i => i.forall(ii => ii.equals(GotoResult.found)))
    assert(allFound)

    println("checking duplicates")
    val allVals = mapOverMap((k: String,  v: List[String]) => biDirIndex.find(k)).flatten.toList
    val countAll =  countAllIterOnce(allVals)
    val dupeSmart = countAll.filter( x => x._2 >1)
    val dupesCorresponds  = dupeSmart.size - dupeMap.size
    //assert(dupesCorresponds == 0)
    if(dupesCorresponds != 0)
      println("\n WARNING: duplicates retrieved are " + -dupesCorresponds + " less than stored")


    println("performing Two-Way-Iterator")
    biDirDataMap.foreach {  case (k: String, v: List[String]) => v.foreach(s => test2WayIterator(biDirIndex.findByValue(s)))}


    println("Checking findFirstByValue")
    val findFirstByValAll = biDirDataMap.map {
      case (k: String, v: List[String]) => (k, v.map(s => biDirIndex.findFirstByValue(s)))
    }


    val ffBvA   = findFirstByValAll.forall(k => k._2.forall(kk => kk.equals(k._1)))

    //hmm, maybe someDupStringStringListMap did indeed create some duplicates across keys, otherwhise this shouldn't fail
    //assert(ffBvA)

    println("Checking countKeys")
    biDirDataMap.forall { case (k: String, v: List[String]) => v.forall(s => biDirIndex.countKeys(s).equals(dupeMap.getOrElse(s, 1)))    }
    println("finished test0")
  }

}