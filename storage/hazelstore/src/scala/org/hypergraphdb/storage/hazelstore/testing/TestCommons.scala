package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.BAtoString
import org.hypergraphdb.handle.UUIDHandleFactory
import org.hypergraphdb.{EmptySearchResult, HGRandomAccessResult, HGStore, HGConfiguration}
import com.hazelcast.core.Hazelcast
import util.Random
import org.hypergraphdb.storage.hazelstore.{Hazelstore3, ByteArrayComparator, HazelStoreConfig, EmptySR}


object TestCommons {
  val sbaconvert = new BAtoString
  val hanGen = new UUIDHandleFactory
  val syncTime = 100
  val baToString = new BAtoString
  type StringListMap = Map[String, List[String]]
  val dataSize = 5

  val random: Random = new Random
  val baComp = new ByteArrayComparator

  def randomString: String = random.nextString(10)

  def stringListLenght: Int = {   val v1 = random.nextInt(500); if (v1 > 10) v1 else stringListLenght   }

  def randomStringList(accuList: List[String] = List.empty[String],
                       length: Int = stringListLenght): List[String] = {
    if (length > 0)
      randomStringList(randomString :: accuList, length - 1)
    else
      accuList
  }


  def getStore(config: HGConfiguration = new HGConfiguration): HGStore = {

    // HAZEL TESTS
    //
    val hs = new Hazelstore3
    config.setStoreImplementation(hs)
    new HGStore("bla", config)
  }

  def test2WayIterator[A](rars:HGRandomAccessResult[A]){
    if (rars.equals(EmptySR))
      return
    else
    {
      assert(try { rars.hasNext } catch { case  t:Throwable => false})
      while(rars.hasNext)   { assert(try { rars.next != null } catch { case  t:Throwable => false}) }
      rars.goAfterLast();
      assert(!(try { rars.hasNext } catch { case  t:Throwable => false}))
      while(rars.hasPrev)   { assert(try { rars.prev() != null } catch { case  t:Throwable => false}) }
      rars.goBeforeFirst()
      assert((try { rars.hasNext } catch { case  t:Throwable => false}))
      assert(!(try { rars.hasPrev } catch { case  t:Throwable => false}))
    }
  }


  def javaRandomStringList(lengthi: Int): java.util.List[String] = {
    var length = lengthi
    var accuList: java.util.List[String] = new java.util.LinkedList[String]()

    while (length > 0) {
      accuList.add(randomString(20).asInstanceOf[String])  // got some weird errors.
      length = length - 1;
    }
    accuList
  }


  // Utility Methods

  type StringMap = Map[String, String]
  val dataMap:StringMap = genStringStringMap(20)
  //  val secondValueKeyMap:StringMap = secondaryValueKeyMap(dataMap)
  def genStringStringMap(count:Int = 20,
                         map:StringMap = Map.empty[String, String],
                         stringGen: String = randomString): StringMap = {
    if (count > 0 ) genStringStringMap(count-1, map.+(randomString -> randomString))
    else map
  }

}
