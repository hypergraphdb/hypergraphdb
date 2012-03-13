package hgtest.storage

import scala.util.Random

object Generators {

  type StringListMap = Map[String, List[String]]
  val random: Random = new Random
  val dataMap2: StringListMap = genStriLiMap()

  def randomStringStream(stringLenght: Int = 10): Stream[String] = Stream.continually(randomString(stringLenght))

  def randomStringList(accuList: List[String] = List.empty[String],
                       length: Int = random.nextInt(30)): List[String] = {
    if (length > 0)
      randomStringList(randomString(20) :: accuList, length - 1)
    else
      accuList
  }

  def javaRandomStringList(lengthi: Int): java.util.List[String] = {
    var length = lengthi
    var accuList: java.util.List[String] = new java.util.LinkedList[String]()

    while (length > 0) {
      accuList.add(randomString(20))
      length = length - 1;
    }
    accuList
  }


  def genStriLiMap(accuMap: StringListMap = Map.empty[String, List[String]],
                   length: Int = random.nextInt(30)): StringListMap = {
    if (length > 0)
      genStriLiMap(accuMap + (randomString(20) -> randomStringList()), length - 1)
    else
      accuMap
  }

  def randomString(length: Int = 10): String = random.nextString(length)

}
