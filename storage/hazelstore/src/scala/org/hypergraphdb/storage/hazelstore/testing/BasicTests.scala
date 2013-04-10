package org.hypergraphdb.storage.hazelstore.testing

import collection.JavaConversions._

import org.hypergraphdb._
import `type`.javaprimitive.StringType
import query.{AtomPartCondition, AtomTypeCondition}
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import storage.ByteArrayConverter
import scala.util.Random
import org.hypergraphdb.HGQuery.hg._
import com.hazelcast.core._
import com.hazelcast.config.{ExecutorConfig, Config}
import java.util.concurrent.{Executors, Executor, Callable}
import org.hypergraphdb.storage.hazelstore._
import org.hypergraphdb.HGQuery.hg

object BasicTests {
  val random = new Random

 def run {main(Array.empty[String])}

 def main (args:Array[String]){

   println("\n\n\nNOW STARTING TEST\n\n\n")

  //graphlesstest(false, false, None)             C
  graphtest(true, false, false)

  println("\n\n\nfinished Test\n\n\n")
 }


  def graphtest(bootstrap:Boolean, transactional:Boolean, async:Boolean){
    println(s"\n\n\nstarting graphtest with parameters bootstrap $bootstrap Transactional $transactional and async $async \n\n\n")

    val conf = new Config
    val graph: HyperGraph = getGraph(hazel = true)
      println("\n\n\n\ngraph instantiated!\n\n\n\n")
     def au[T](t: T): HGHandle = assertAtom(graph, t)
     def ad[T](t: T): HGHandle = graph.add(t)
     def gh[T](t: T): HGHandle = graph.getHandle(t)


       if(bootstrap)
     {
       println("now in bootstrap")
       val hallo = "hallo"
       val halloH = graph.add(hallo)
       val welt = "Welt"
       val weltH = graph.add(welt)
       val halloHH = graph.getHandle(hallo)
       val weltHH = graph.getHandle(welt)
       assert(weltH == weltHH)
       assert(halloH == halloHH)

       //val links = (0 to 10).map(i => graph.add(new WhenThen(au(i), au(i + 1)))).toSeq
       val links = (0 to 10).map(i => graph.add(new HGPlainLink(au(i), au(i + 1)))).toSeq
       println("\n\n\nnow Adding Persons\n\n\n")
       val persons                  = (1 to 10).map(i => graph.add(new Person(random.nextString(i),random.nextString(i),random.nextString(i),i)))
       println("\n\n\nFinised adding Persons\n\n\n")
     }

     hg.getOne(graph,new AtomTypeCondition(classOf[Person]))

     val incid = (0 to 10).map(i => graph.getIncidenceSet(au(i)))        // ok
     val linksBack = (0 to 10).map(i => getAll(graph, link(gh(i))))     // ok
     //  assert(links.corresponds(linksBack)(_==_))

     val x = graph.find(apply(targetAt(graph, 1),orderedLink(anyHandle(), gh(2))))

    println(" personsBackByINsuranceID 1 to 7")
    (1 to 7).map(i => getAll(graph, and(new AtomTypeCondition(classOf[Person]), new AtomPartCondition("insurance".split("\\."), i)))).foreach(println)

    println(" personsBackByINsuranceID gt than 5")
    getAll(graph, and(new AtomTypeCondition(classOf[Person]), gt("insurance", 5))).foreach(println)


    getAll(graph, new AtomTypeCondition(classOf[String])).foreach(println)
    getAll(graph, new AtomTypeCondition(classOf[Integer])).foreach(println)

    // println(graph.get(halloH))
    // graph.close()

    println(s"\n\n\n\nSucessfully finished graphtest with parameters bootstrap $bootstrap Transactional $transactional and async $async \n\n\n\n")
 }

  def getGraph(hazel:Boolean = true, transactional : Boolean = true, hazelstoreConfig:HazelStoreConfig = new HazelStoreConfig()):HyperGraph = {

    val graph = new HyperGraph()
    val config = new HGConfiguration
    config.setTransactional(false)
    config.setUseSystemAtomAttributes(false)

    if(hazel)
      config.setStoreImplementation(new Hazelstore3(hazelstoreConfig))

    graph.setConfig(config)
    println("\n\n\n\nnow trying to instantiate graph \n\n\n\n")
    graph.open("/home/ingvar/bin/trunk/bje/")
    graph
  }

  def graphlesstest(transactional:Boolean, async:Boolean, hi:Option[HazelcastInstance]) {
    println(s"starting graphlesstest with parameters Transacional $transactional and async $async")
    val config = new HGConfiguration
    config.setTransactional(transactional)
    val hs = new Hazelstore3()
    config.setStoreImplementation(hs)
    val store = new HGStore("bla", config)
    val baToString: ByteArrayConverter[String]= new StringType
    val index = store.getIndex(random.nextString(10), baToString, baToString, BAComp, true)

    def testStore{

      def testDataStoreContainsGetRemoveGet {
        val hw = "hallo Welt"
        val ba = store.store(hw.getBytes)
        assert(store.containsData(ba))
        assert(new String(store.getData(ba)).equals(hw))
        store.removeData(ba)
        assert(store.getData(ba)==null)
      }

      def testLinkStoreContainsGetRemoveGet {
        val h1 = store.store("val1".getBytes)
        val h2 = store.store("val2".getBytes)
        val li = Array[HGPersistentHandle](h1, h2)
        val liH = store.store(li)
        assert(store.containsLink(liH))
        val liB = store.getLink(liH)
        assert(eq(li, liB))
        store.removeLink(liH)
        assert(store.getLink(liH) == null)
      }

      testDataStoreContainsGetRemoveGet
      testLinkStoreContainsGetRemoveGet
    }

    def testIndex{

      val k1 = "key1"
      val k2 = "key2"

      index.addEntry(k1, "value1")

      index.addEntry(k1, "value2")
      Thread.sleep(100)
      index.removeEntry(k1, "value2")
      Thread.sleep(1000)
      index.addEntry(k1, "value2")
      index.addEntry(k1, "value3")
      index.addEntry(k1, "valueX")
      index.removeEntry(k1, "valueX")

      //Thread.sleep(1000)

      index.addEntry(k2, "value4")
      Thread.sleep(100)

      val k1back= index.find(k1)
      val countk1 = index.count(k1)
      assert(countk1==3)
      assert(index.count(k2)==1)
      val count = index.count
      assert( count == 2 )
      val keys = index.scanKeys()
      assert(keys.length == 2)

      val scanVals1 = index.scanValues()
      val scanValsLength = scanVals1.length
      assert(scanValsLength == 4)
      assert(foundValueX(List(1,2,3,4),scanVals1))

      val k1vals = index.find(k1)
      assert(foundValueX(List(1,2,3),k1vals))
      assert(notFoundValueX(List(4),k1vals))    //leads to an endless loop in binarysearch

      // JAVACONVERSIONS DONT WORK RELIABLY!!!           // confirmed manually that scanKey contains both k1 and k2

      index.removeEntry(k1,"value2")

      Thread.sleep(100)
      val scanVals2: HGRandomAccessResult[String] = index.find(k1)
      assert(foundValueX(List(1,3), scanVals2))
      assert(notFoundValueX(List(2,4),scanVals2))      //leads to an endless loop in binarysearch

      assert(index.count(k1)==2)

      val scanVals3: HGRandomAccessResult[String] = index.scanValues()
      assert(List(1,3).forall( i => scanVals3.goTo("value"+i.toString, true) == GotoResult.found))
      assert(foundValueX(List(1,3),scanVals3))
      assert(notFoundValueX(List(2), scanVals3))

      index.removeAllEntries(k1)
      val scanVals4 = index.find(k1)
      Thread.sleep(1000)
      assert(notFoundValueX(List(1,2,3,4),scanVals4))
      val scanVals5 = index.scanValues()
      assert(notFoundValueX(List(1,2,3),scanVals5))
      assert(foundValueX(List(4),scanVals3))

      val count1 = index.count(k1)
      assert(count1.equals(null) || count1 == 0)
      assert(index.count == 1)
      assert(index.scanValues().length == 1 )

      printall
    }
    def testBidirIndex {}


    testStore
     testIndex
 //      testBidirIndex


    def printall{
       println("KEYS\n")
       index.scanKeys().foreach(println)
       println("VALUES\n")
       index.scanValues().foreach(println)

     }

    def eq[T](left: Array[T], right: Array[T]): Boolean = {
      if (left.equals(right))  true
      else if (left == null || right == null)  false
      else if (left.length != right.length)  false
      else  ( left.deep ==   right.deep) && (left.corresponds(right)(_ == _))
    }

    def foundValueX(i:Seq[Int], rs: HGRandomAccessResult[String]):Boolean =
      i.forall( i => rs.goTo("value"+i.toString, true) == GotoResult.found)

    def notFoundValueX(i:Seq[Int], rs: HGRandomAccessResult[String]):Boolean =
        i.forall( i => rs.goTo("value"+i.toString, true) == GotoResult.nothing)


    println(s"\n\n\n\nsuccessfully finished Testgraphlesstest with parameters Transacional $transactional and async $async\n\n\n\n ")

  }

  def countIt[I[_] <: Iterator[_]](it:I[_], ac:Int = 0):Int = if (! it.hasNext) ac else countIt(it, ac+1)
}
