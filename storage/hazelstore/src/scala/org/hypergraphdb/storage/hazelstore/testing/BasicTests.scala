package org.hypergraphdb.storage.hazelstore.testing


import org.hypergraphdb._
import `type`.javaprimitive.StringType
import storage.ByteArrayConverter
import org.hypergraphdb.storage.hazelstore._
import TestCommons._
import com.hazelcast.core.{PartitionAware, Hazelcast}
import java.util.concurrent.Callable
import collection.JavaConversions._
import org.hypergraphdb.storage.hazelstore.StoreCallables.{AddRunnable, AddCallable}
import com.hazelcast.config.Config


object BasicTests {
 def run {main(Array.empty[String])}

 def main (args:Array[String]){
   implicit val testDataSize = TestCommons.dataSize

   /*
   val hi = Hazelcast.newHazelcastInstance()
   val callableMap = hi.getMap[Array[Byte],String]("callable")
   val runnableMap = hi.getMap[Array[Byte],String]("runnable")

   callableMap.put("bla".getBytes(), "bla")
   val res1 = callableMap.get("bla".getBytes())
   callableMap.remove("bla".getBytes)
   val res2 =  callableMap.get("bla".getBytes())

   println("now printing callable keySet")
   val callableMapKeySet = callableMap.keySet()
   callableMapKeySet .foreach(println)
   println("now printing callable keySet size" + callableMapKeySet .size)

   println("now printing runnable keySet")
   val runnableMapKeySet = runnableMap.keySet()
   runnableMapKeySet.foreach(println)
   println("now printing callable keySet size" + runnableMapKeySet.size)


   val exec = hi.getExecutorService

   (1 to 2000).foreach(i => {
     exec.submit(new AddCallable(random.nextString(10).getBytes()))
     exec.execute(new AddRunnable(random.nextString(10).getBytes()))
     //exec.execute(new Add(new BAW(random.nextString(10).getBytes())))
     //exec.execute(new Add(i))
   })

   println("bla")
   println("bla")
   hi.getLifecycleService.shutdown()
//   Hazelcast.shutdownAll()
   */


   def test(f: HazelStoreConfig => Long) = configPermutations.map(c => {
     println(s"\n\n\nNow running config $c\n\n\n");Thread.sleep(1333)
     val run = f(c)
     println(s"\n\n\nRunning config $c\n took: " +run)
     Thread.sleep(1333)
     (c.toString, run)
   })

   println("\n\nG R A P H T E S T\n\n")
   val graphResults = test((c:HazelStoreConfig) => new GraphTest(  getGraph(getConfig(c)),bootstrap = true).run)

   println("\n\nI N D E X\n\n")
   val indexResults = test((c:HazelStoreConfig) => new IndexTest(getIndex(getStore(getConfig(c))),c.async).run)

   println("\n\nS T O R A G E   T E S T\n\n")
   val storeResults = test((c:HazelStoreConfig) => new StorageTest(getStore(getConfig(c)),c.async).run)

   println("\n\nB I D I R E C T I O N A L   I N D E X\n\n")
   val bidirAsIndex = test((c:HazelStoreConfig) => new IndexTest(getBidirectionalIndex(getStore(getConfig(c))),c.async).run)
   val bidirResults = test((c:HazelStoreConfig) => new BiDirTest2(getBidirectionalIndex(getStore(getConfig(c))),c.async).run)




   println(s"\n\n\nRESULTS  normalized to datasize $testDataSize")
   println("now printing Results of StoreTests:")
   storeResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("now printing Results of IndexTests:")
   indexResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("now printing Results of bidirAsIndex:")
   bidirAsIndex.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("now printing Results of bidirResults:")
   bidirResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("\nnow printing Results of GraphTests:")
   graphResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
 }


  def getConfig(hazelConfig:HazelStoreConfig):HGConfiguration = {
    val config = new HGConfiguration
    config.setTransactional(false)
    config.setUseSystemAtomAttributes(false)
    val hs = new Hazelstore(hazelConfig)
    config.setStoreImplementation(hs)
    config
  }

  def getStore(config:HGConfiguration):HGStore = new HGStore("bla", config)
  def getIndex(store:HGStore):HGIndex[String, String] = {
    val baToString: ByteArrayConverter[String]= new StringType
    store.getIndex(random.nextString(10), baToString, baToString, BAComp, true)


  }
  def getBidirectionalIndex(store:HGStore):HGBidirectionalIndex[String, String] = {
    val baToString: ByteArrayConverter[String]= new StringType
    store.getBidirectionalIndex(random.nextString(10), baToString, baToString, BAComp, true)
  }

  def getGraph(config:HGConfiguration):HyperGraph = {
    val graph = new HyperGraph()
    graph.setConfig(config)
    graph.open("")
    graph
  }
}
