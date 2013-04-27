package org.hypergraphdb.storage.hazelstore.testing

import scala.collection.immutable
import org.hypergraphdb.handle.UUIDHandleFactory
import org.hypergraphdb.{HGHandleFactory, HGPersistentHandle}

/**
 * User: Ingvar Bogdahn
 * Date: 21.04.13
 */
object Generators {
  import TestCommons._

  object Handles{
    implicit val handleFactory = new UUIDHandleFactory
    def mkHandle = handleFactory.makeHandle()
    def mkHandles(a:Int)(implicit handleFactory : HGHandleFactory):Array[HGPersistentHandle] = (0 to a).map(i => mkHandle).toArray
    def mkHandleHandleSeqPair = (mkHandle,mkHandles(random.nextInt(dataSize)))
    def mkHandleBArrayPair = {
      val ba = new Array[Byte](random.nextInt(dataSize))
      random.nextBytes(ba)
      (mkHandle,ba)
    }
  }

  object Strings{


  def randomString: String = random.nextString(10)

  def stringListLenght: Int = {   val v1 = random.nextInt(dataSize); if (v1 != 0) v1 else dataSize   }

  def randomStringList(accuList: List[String] = List.empty[String],
                       length: Int = stringListLenght): List[String] = {
    if (length > 0)
      randomStringList(randomString :: accuList, length - 1)
    else
      accuList
  }

  def genStriLiMap(accuMap: Map[String, List[String]] = Map.empty[String, List[String]],
                   length: Int = dataSize): Map[String, List[String]] = {
    if (length > 0)
      genStriLiMap(accuMap + (randomString -> randomStringList()), length - 1)
    else
      accuMap
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


  // Utility Methods

  type StringMap = Map[String, String]

  def genStringStringMap(count:Int = 20,
                         map:StringMap = Map.empty[String, String],
                         stringGen: String = randomString): StringMap = {
    if (count > 0 ) genStringStringMap(count-1, map.+(randomString -> randomString))
    else map
  }
  }

}
