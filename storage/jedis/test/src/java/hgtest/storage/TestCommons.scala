package hgtest.storage

import scala.util.Random
import org.hypergraphdb.storage._
import bdb.BDBStorageImplementation
import org.hypergraphdb.storage.redis.JedisStorImpl
import org.hypergraphdb.storage.redis.ByteArrayComparator
import org.hypergraphdb.{HyperGraph, HGRandomAccessResult, HGConfiguration, HGStore}
import org.hypergraphdb.storage.redis.ByteArrayComparator
import collection.JavaConversions._


object TestCommons {
  val baToString = new BAtoString;
  type StringListMap = Map[String, List[String]]
  var dataSize = 50

  val random: Random = new Random
  val baComp = new ByteArrayComparator
  //val store = getStore()

  def setDataSize(newDataSize: Int):Unit = {dataSize = newDataSize}

  def randomString: String = random.nextString(10)

  def stringListLenght: Int = {
    val v1 = random.nextInt(500); if (v1 > 10) v1 else stringListLenght
  }

  def randomStringList(accuList: List[String] = List.empty[String],
                       length: Int = stringListLenght): List[String] = {
    if (length > 0)
      randomStringList(randomString :: accuList, length - 1)
    else
      accuList
  }


  def getStore(config: HGConfiguration = new HGConfiguration): HGStore = {
    //config.setStoreImplementation(new JedisStorImpl);
    //new HGStore("192.168.1.6", config)

    config.setStoreImplementation(new BDBStorageImplementation);
    return new HGStore("/home/ingvar/workspace/hgdb2/mixstordbdir", config)

  }

  def test2WayIterator[A](rars:HGRandomAccessResult[A]):Unit = {

    assert(try { rars.hasNext } catch { case  t:Throwable => false})
    while(rars.hasNext)   { assert(try { rars.next != null } catch { case  t:Throwable => false}) }
    rars.goAfterLast();
    assert(!(try { rars.hasNext } catch { case  t:Throwable => false}))
    while(rars.hasPrev)   { assert(try { rars.prev() != null } catch { case  t:Throwable => false}) }
    rars.goBeforeFirst()
    assert((try { rars.hasNext } catch { case  t:Throwable => false}))
    assert(!(try { rars.hasPrev } catch { case  t:Throwable => false}))
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



}


/* RECYCLE:

  def initializeGraph(host:String, port: String): HyperGraph = {
    var config: HGConfiguration = new HGConfiguration
    //config.setStoreImplementation(new JedisStorImpl)
    /*    config.setTransactional(false)
     val graph = new HyperGraph
     graph.setConfig(config)
     graph.open(host)
    */

    config.setStoreImplementation((HGStoreImplementationnew) BDBStorageImplementation);
    val graph = new HyperGraph("/home/ingvar/workspace/intellij/jedisHGDB/")
    graph
  }


   // Utility Methods

  type StringMap = Map[String, String]
  val dataMap:StringMap = genStringStringMap(20)
  val secondValueKeyMap:StringMap = secondaryValueKeyMap(dataMap)
  def genStringStringMap(count:Int = 20,
                         map:StringMap = Map.empty[String, String],
                         stringGen: String = randomString): StringMap = {
        if (count > 0 ) genStringStringMap(count-1, map.+(randomString -> randomString))
        else map
      }



//  def genStringList(count:Int = 20): List[String] = {if(count >1) randomString :: genStringList (count -1 ) else randomString :: Nil}
def functionSucceded(f: () => Unit):Boolean = {
    var succeed= false
    try { f; succeed = true }
    catch {case t:Throwable => println("throwable catched"); return false }
    return succeed
  }

doesn't work:
  def functionSucceded(f: () => Unit):Boolean = {
    return try { f; true }
    catch {case t:Throwable => return false; case _ => return false }
  }




*/