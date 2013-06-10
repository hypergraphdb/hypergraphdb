package org.hypergraphdb.storage.hazelstore.testing


import org.hypergraphdb._
import org.hypergraphdb.storage.hazelstore._
import TestCommons._
import com.hazelcast.core.Hazelcast
import collection.JavaConversions._
import org.hypergraphdb.storage.hazelstore.StoreCallables.{AddRunnable, AddCallable}
import org.hypergraphdb.HGQuery.hg


object BasicTests {
 def run {main(Array.empty[String])}

 def main (args:Array[String]){
   implicit val testDataSize = TestCommons.dataSize

   /*
   val graph = getGraph(getHGConfig(null))
   val a = timeMeasure((0 to 400000).foreach(graph.add))
   val b = timeMeasure((0 to 400000).foreach(hg.assertAtom(graph, _)))
   graph.close


   println("test took " + a._1 + " and " + b._1)
*/
   def test(f: HazelStoreConfig => Long) = configPermutations.map(c => {
     println(s"\n\nNow running config $c\n\n");
     val run = f(c)
     println(s"\n\nRunning config $c\n took: " +run)
     (c.toString, run)
   })

   //   T E S T S    G O    H E R E

   println("\n\nG R A P H T E S T\n\n")
   val graphResults = test((c:HazelStoreConfig) => new GraphTest(  getGraph(getHGConfig(c)),bootstrap = true).run)

   println("\n\nI N D E X\n\n")
   val indexResults = test((c:HazelStoreConfig) => new IndexTest(getIndex(getStore(getHGConfig(c))),c.async).run)

   println("\n\nB I D I R E C T I O N A L   I N D E X\n\n")
   val bidirAsIndex = test((c:HazelStoreConfig) => new IndexTest(getBidirectionalIndex(getStore(getHGConfig(c))).asInstanceOf[HGSortIndex[String, String]],c.async).run)
   val bidirResults = test((c:HazelStoreConfig) => new BiDirTest2(getBidirectionalIndex(getStore(getHGConfig(c))),c.async).run)

   println("\n\nS T O R A G E   T E S T\n\n")
   val storeResults = test((c:HazelStoreConfig) => new StorageTest(getStore(getHGConfig(c)),c.async).run)


   // E N D   T E S T   D E C L A R A T I O N


   //Seq.empty[(String,Long)]  //

   println(s"\n\n\nRESULTS  normalized to datasize $testDataSize")
   println("Results of StoreTests:")
   storeResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("Results of IndexTests:")
   indexResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("Results of bidirAsIndex:")
   bidirAsIndex.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("Results of bidirResults:")
   bidirResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
   println("\nResults of GraphTests:")
   graphResults.sortBy(_._2).foreach(i => {println(i._1);println(i._2 / testDataSize)})
 }

  def hazeltests{   val hi = Hazelcast.newHazelcastInstance
    val callableMap = hi.getMap[Array[Byte],String]("callable")
    val runnableMap = hi.getMap[Array[Byte],String]("runnable")

    callableMap.put("bla".getBytes, "bla")
    val res1 = callableMap.get("bla".getBytes)
    callableMap.remove("bla".getBytes)
    val res2 =  callableMap.get("bla".getBytes)

    println("callable keySet")
    val callableMapKeySet = callableMap.keySet
    callableMapKeySet .foreach(println)
    println("callable keySet size" + callableMapKeySet .size)

    println("runnable keySet")
    val runnableMapKeySet = runnableMap.keySet
    runnableMapKeySet.foreach(println)
    println("callable keySet size" + runnableMapKeySet.size)


    val exec = hi.getExecutorService

    (1 to 2000).foreach(i => {
      exec.submit(new AddCallable(random.nextString(10).getBytes))
      exec.execute(new AddRunnable(random.nextString(10).getBytes))
      //exec.execute(new Add(new BAW(random.nextString(10).getBytes)))
      //exec.execute(new Add(i))
    })

    println("bla")
    println("bla")
    hi.getLifecycleService.shutdown
    //   Hazelcast.shutdownAll
  }

}
/*
val testSize = 100000
val graph = getGraph(getHGConfig(null))
timeMeasure(graph.bulkImport((0 to testSize),null))                    // warmup
val meas1 = timeMeasure(graph.bulkImport((testSize to (2*testSize)),null))
timeMeasure(graph.bulkImportIfAbsentInCache((0 to -testSize),null))     // warmup and pre-store
val meas2 = timeMeasure(graph.bulkImportIfAbsentInCache((0 to -testSize),null))
val meas3 = timeMeasure((0 to -testSize).foreach(i => HGQuery.hg.assertAtom(graph, i)))


println("bulk Import took " + meas1 + " whereas bulk import of previously stored took" + meas2 + " whereas assertAtom of previously stored took" + meas3+ " nanos.")

graph.close
*/