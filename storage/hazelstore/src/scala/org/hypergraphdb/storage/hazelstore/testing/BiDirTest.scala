package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.hazelstore.{Hazelstore, HazelStoreConfig}
import org.hypergraphdb.{HGRandomAccessResult, HGStore, HGConfiguration}
import collection.immutable
import collection.JavaConversions._
import com.hazelcast.core.Hazelcast
import scala.Some
import org.hypergraphdb.HGRandomAccessResult.GotoResult


object BiDirTest {

  import TestCommons._
  import Generators.Strings._


  val random = new scala.util.Random
  val hs = new Hazelstore(new HazelStoreConfig)
  //hs.getConfiguration.asInstanceOf[HazelStoreConfig].getHazelConfig.getExecutorConfig.setCorePoolSize(100)
  val config:HGConfiguration = new HGConfiguration
  config.setStoreImplementation(hs)
  config.setTransactional(false)
  val store = new HGStore("bla", config)
  //hs.startup(store,config)
  val biDirIndex = store.getBidirectionalIndex("index1", baToString, baToString, baComp, true)
  val biDirIndex2 = store.getBidirectionalIndex("index2", baToString, baToString, baComp, true)


  val dupeList:     List[String]        = randomStringList(length = dataSize)
  val dupeRegister: Map[String, Int]    = dupeList.map(s => (s, 0)).toMap[String, Int]

  // biDirDataMap corresponds to the index
  // dupeMap keeps tracks of the duplicates
  val (biDirDataMap, dupeMap) :(immutable.Map[String, List[String]],immutable.Map[String,Int])   =
    someDupStringStringListMap(immutable.Map.empty[String, List[String]], dupeRegister, dataSize)

  def main(args: Array[String]) {

    println("now adding and removing data in separate index for warming up")
    biDirDataMap.take(5).foreach { case (k: String, v: List[String]) => v.foreach(s => biDirIndex2.addEntry(k, s))    }
    biDirDataMap.take(3).foreach { case (k: String, v: List[String]) => v.foreach(s => biDirIndex2.removeEntry(k, s))   }
    biDirDataMap.drop(3).take(2).foreach { case (k: String, v: List[String]) => v.foreach(s => biDirIndex2.removeAllEntries(k))   }


    println("\n\n\nnow starting Test")
    val start = System.nanoTime()
    tests
    val stop = System.nanoTime()
    println("test took " + ((stop -start)/1000000) + " milliseconds")
    Hazelcast.shutdownAll()

  }

  def someDupStringStringListMap( biDirDataAccuMap: immutable.Map[String, List[String]],
                                  dupeMap: immutable.Map[String,Int],
                                  length: Int):
  (immutable.Map[String, List[String]],immutable.Map[String,Int]) =
  {
    if (length > 0)
      if (random.nextBoolean() && random.nextBoolean() )  // make frequency of dupes tuneable
      {
        val rand = random.nextInt(dupeMap.size - 1)
        val dupe = dupeMap.keysIterator.drop(rand-1).next
        val dupeOldCount = dupeMap(dupe)
        someDupStringStringListMap(biDirDataAccuMap ++ immutable.Map((randomString, ( dupe :: randomStringList()))), dupeMap.updated(dupe,dupeOldCount+1), length - 1)
      }
      else
        someDupStringStringListMap(biDirDataAccuMap + (randomString -> randomStringList()),dupeMap, length - 1)
    else
      (biDirDataAccuMap, dupeMap)
  }

  // counts occurences of Elements in a List
  def countAllIterOnce[T](l:List[T], countMap:immutable.Map[T,Int] = immutable.Map.empty[T,Int]):immutable.Map[T,Int] =
    if (l.isEmpty) countMap
    else countMap.get(l.head) match  {
      case None     =>  countAllIterOnce(l.tail, countMap ++ immutable.Map((l.head, 1)))
      case Some(a)  =>  countAllIterOnce(l.tail, countMap.updated(l.head,a + 1))
    }

  def mapOverMap[R](fun: (String, List[String]) => R) = biDirDataMap.map {case (k: String, v: List[String]) => fun(k,v)}

  val allOrigValues = mapOverMap((k: String, v: List[String]) => v.map(vv =>vv)).flatten.toList
  val allOrigKeys = mapOverMap((k: String, v: List[String]) => k)
  val allOrigValuesSet = allOrigValues.toSet
  val allOrigKeysSet = allOrigKeys.toSet

  def randomFilter[A](a: A)(implicit random: scala.util.Random): Boolean = random.nextBoolean()


  def tests{

    println("Adding StringKey-StringTuple pairs to Index, with some duplicate values occuring across several keys")
    biDirDataMap.foreach { case (k: String, v: List[String]) => v.foreach(s => biDirIndex.addEntry(k, s))    }
    //since duplicate don't seem to be added, adding some dupes manually now
    ( 0 to dataSize-1).foreach(i =>
      //biDirDataMap.iterator.drop(random.nextInt(dataSize/2)). take(random.nextInt(dataSize/2)).............
    biDirDataMap.foreach{ case (k: String, v: List[String]) => if(random.nextBoolean()) biDirIndex.addEntry(k,dupeList(i))})
    //Thread.sleep(5000)
//    val count2 = biDirIndex.count()
      val count2 = repeatUntil1(biDirIndex.count)(_ == dataSize)
    assert(count2._3)

    println("Checking found keys from findByValue(ALLVALUES, exactValue).goto(key, exact=true)")
//    val keysValuesFoundKeys = mapOverMap((k: String,  v: List[String]) => v.map(vv => (k,vv,biDirIndex.findByValue(vv))))
//    val keysValuesFoundKeys = biDirDataMap.map {case (k: String, v: List[String]) => v.map(vv => (k,vv,biDirIndex.findByValue(vv)))}; assert(keysValuesFoundKeys.size.equals(biDirDataMap.size))

    val foundKeys = biDirDataMap.map {
      case (k: String, v: List[String]) => v.map(vv => biDirIndex.findByValue(vv).goTo(k, true))
    }
    val allFound = foundKeys.forall(i => i.forall(ii => ii.equals(GotoResult.found)))
    assert(allFound)

    println("checking it has all keys")
    val scanKeys = biDirIndex.scanKeys()
    val stringified = scanKeys.map(i => i).toList
    val allFoundK = biDirDataMap.map(key => scanKeys.goTo(key._1, true))
    assert(allFoundK.forall(_.equals(GotoResult.found)))


    println("checking it has all values")
    //val scanValues        =  biDirIndex.scanValues()
    //val allFoundV =   allOrigValuesSet.forall(key => scanValues.goTo(key, true).equals(GotoResult.found))
    val allFoundV        =  repeatUntil1(biDirIndex.scanValues)( (scanValues:HGRandomAccessResult[String]) => allOrigValuesSet.forall(key => scanValues.goTo(key, true).equals(GotoResult.found)))
    assert(allFoundV._3)

    println("checking duplicates")
    val allVals = mapOverMap((k: String,  v: List[String]) =>biDirIndex.find(k)).flatten.toList
    val countAll =  countAllIterOnce(allVals)
    val dupeSmart = countAll.filter( x => x._2 >1)
    val dupesCorresponds  = dupeSmart.size - dupeMap.size
    assert(dupesCorresponds == 0)
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
    assert(ffBvA)

    println("Checking countKeys")
    biDirDataMap.forall { case (k: String, v: List[String]) => v.forall(s => biDirIndex.countKeys(s).equals(dupeMap.getOrElse(s, 1)))    }
    println("finished test0")
  }
}


/*

 def genStringListMapSomeWithDupes(biDirDataAccuMap: Map[String, List[String]] = Map.empty[String, List[String]],
                                                        length: Int = 40): Map[String, List[String]] = {
  if (length > 0)
    if (random.nextBoolean()) {//randomly insert some duplicates {
    //          genStringListMapSomeWithDupes(biDirDataAccuMap + (randomString -> (dupeList.drop(random.nextInt(dupeList.size-2)) ::: randomStringList())), length - 1)
    val rand = random.nextInt(dupeList.size - 2)
      dupeMap.drop(rand).foreach {
        case (k: String, v: Int) => dupeMap.+(k -> (v + 1))
      }
      val tempMap = dupeMap.drop(rand)
      val tempList = tempMap.keys.toList
      genStringListMapSomeWithDupes(biDirDataAccuMap + (randomString -> (tempList ::: randomStringList())), length - 1)}

    else
      genStringListMapSomeWithDupes(biDirDataAccuMap + (randomString -> randomStringList()), length - 1)
  else
    biDirDataAccuMap

}
*/