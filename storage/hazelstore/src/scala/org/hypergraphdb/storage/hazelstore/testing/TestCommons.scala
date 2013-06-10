package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.{HGStoreImplementation, ByteArrayConverter, BAtoString}
import org.hypergraphdb.handle.{SequentialUUIDHandleFactory, UUIDHandleFactory}
import org.hypergraphdb._
import org.hypergraphdb.storage.hazelstore._
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import scala.collection.immutable
import scala.util.{Try, Random}
import org.hypergraphdb.`type`.javaprimitive.StringType
import scala.annotation.{tailrec, elidable}
import scala.annotation.elidable._
import com.hazelcast.config.Config
import org.hypergraphdb.storage.mapStore.{HGIncidenceDB, HGLinkDB, HGDataDB, MapStoreImpl}


object TestCommons {
  val sbaconvert = new BAtoString
  val hanGen = new UUIDHandleFactory
  val syncTime = 20
  val baToString = new BAtoString
  type StringListMap = Map[String, List[String]]
  val dataSize = 30

  val configPermutationsBase :Seq[HazelStoreConfig] = Seq(
  {log("\n\nJ V M   W A R M   U P   R U N ");new HazelStoreConfig().setAsync(false).setUseTransactionalCallables(false)},
  //new HazelStoreConfig().setAsync(true).setUseTransactionalCallables(false),
    new HazelStoreConfig().setAsync(true).setUseTransactionalCallables(true),
  new HazelStoreConfig().setAsync(false).setUseTransactionalCallables(false),
  new HazelStoreConfig().setAsync(false).setUseTransactionalCallables(true)

  )

  // U S E   L I T E   H A Z E L C A S T   I N S T A N C E
  val lite = true

  val liteMember:Config=new Config().setLiteMember(true)

  val configPermutations :Seq[HazelStoreConfig] = if (lite) configPermutationsBase.map(hsc => hsc.setHazelcastConfig(liteMember)) else configPermutationsBase


  def setupStorImp(si:HGStoreImplementation){
    val hgconfig = new HGConfiguration
    hgconfig.setTransactional(false)
    si.startup(null, hgconfig)
  }




  //def repeatUntil[I,R](fun: I => R, i:I)(until: R => Boolean):(R,Long, Boolean) = repeatUntilBase(fun(i))(until)(0)(syncTime)
  def repeatUntil1[R](fun:() => R)(until: R => Boolean):(R,Long, Boolean) = repeatUntilBase(fun)(until)(0)(syncTime)
  private def repeatUntilBase[R](fun: ()=> R)(until: R => Boolean)(runTime:Long)(timeOut:Long = syncTime):(R,Long, Boolean) = {
    val start = System.currentTimeMillis()
    val res = fun()
    val ready = until(res)
    val localRunTime = System.currentTimeMillis()-start
    (ready, (localRunTime + runTime) >= timeOut) match {
      case (true,_)       => (res, (localRunTime) + runTime, true)
      case (false, false) => {
        Thread.sleep(localRunTime)
        //log("repeatUntil is repeating...")
        repeatUntilBase(fun)(until)(localRunTime + runTime)(timeOut)
      }
      case (false, true)  => {
        log("repeatUntil gave up retrying...")
        (res, (System.currentTimeMillis()-start) + runTime, false)}
    }
  }

  def repeatUntil[I,R](fun: I => R, i:I)(until: R => Boolean, timeOut:Long = syncTime):(R,Long, Boolean) = {
    val startTime = System.currentTimeMillis()
    var cur : R = fun(i)
    var untilTrue:Boolean = until(cur)
    while( !untilTrue && System.currentTimeMillis() - startTime < timeOut)
    {
      //log("repeating." + (System.currentTimeMillis() - startTime) + "  millis passed" );
      Thread.sleep(System.currentTimeMillis() - startTime)
      cur = fun(i)
      untilTrue = until(cur)
    }
    if(!untilTrue)
    {   println("repeatUntil gave up retrying" );
      (cur, System.currentTimeMillis() - startTime, false)}
    else
      (cur, System.currentTimeMillis() - startTime, true)
  }


  @tailrec
  def waitFor (waitTime:Long = syncTime * 1000000) {
    if(waitTime <= 0) Unit
    else waitFor(waitTime -1)
    //val start = System.nanoTime()
    //while(System.nanoTime() - start < waitTime){Unit}
  }

  val random: Random = new Random
  val baComp = new ByteArrayComparator

  def mkValidt[Tested,Data](testName:String, fun:(Tested,Seq[Data]) => Boolean) =
    (testName,  (store:Tested, data:Seq[Data]) => Try{
      val a = fun(store,data)
      assert(a, testName + "failed")
      a
    })

  def mkValidtForAll[Tested,Data](testName:String, fun:((Tested,Data) => Boolean)):(String, (Tested,Seq[Data]) => Try[Boolean])=
    (testName,  (store:Tested, data:Seq[Data]) => Try{
      val a = data.forall(dataSet => fun(store,dataSet))
      assert(a, testName + "failed")
      a
    })


  @elidable(INFO) def log(s:String) = println(s)

  def RSMatchGoTo[T](i:Seq[T], rs: HGRandomAccessResult[T], matchResult:GotoResult):Boolean =
    i.forall( i => rs.goTo(i, true) == matchResult)

  def countIt[I[_] <: Iterator[_]](it:I[_], ac:Int = 0):Int = if (! it.hasNext) ac else countIt(it, ac+1)

  def timeMeasure[R](f: => R):(Long,R) = {
    val start = System.nanoTime
    val r = f
    ((System.nanoTime - start),r)
  }


  def countAllIterOnce[T](l:List[T], countMap:immutable.Map[T,Int] = immutable.Map.empty[T,Int]):immutable.Map[T,Int] =
    if (l.isEmpty) countMap
    else countMap.get(l.head) match  {
      case None     =>  countAllIterOnce(l.tail, countMap ++ immutable.Map((l.head, 1)))
      case Some(a)  =>  countAllIterOnce(l.tail, countMap.updated(l.head,a + 1))
    }

  def getHGConfig(hazelConfig:HazelStoreConfig):HGConfiguration = {
    val config = new HGConfiguration
    //config.setAtomCacheFunction(new HGFunction1[HyperGraph,HGAtomCache]{def apply(arg: HyperGraph): HGAtomCache = new SimpleAtomCache() })
    //val handleFactory = new SequentialUUIDHandleFactory(System.currentTimeMillis(), 0);
    //config.setHandleFactory(handleFactory);
    config.setTransactional(false)
    config.setUseSystemAtomAttributes(false)
    //config.setMaxCachedIncidenceSetSize(100)
    //config.setSkipMaintenance(true)
    //config.getStoreImplementation.getConfiguration.asInstanceOf[BJEConfig].getEnvironmentConfig.setCacheSize(1024*1024*500)
    config.setStoreImplementation(new Hazelstore(hazelConfig))

/*    val ms:HGStoreImplementation = new MapStoreImpl with HGDataDB with HGLinkDB with HGIncidenceDB
    {
      val datadb: java.util.Map[DKey, DVal] = genMap.getMap[DKey, DVal]("datadb").get
      val linkdb: java.util.Map[LKey, LVal] = genMap.getMap[LKey,LVal]("linkdb").get
      val incidenceDB: java.util.Map[IKey, IVal] = genMap.getMap[IKey, IVal]("incidenceDB").get//.asInstanceOf[util.Map[IKey, IVal]]
    val handleSize = 16
   }
    config.setStoreImplementation(ms)*/
    config.setStoreImplementation(new Hazelstore(hazelConfig))
    config
  }

  def getStore(config:HGConfiguration):HGStore = new HGStore("bla", config)
  def getIndex(store:HGStore):HGSortIndex[String, String] = {
    val baToString: ByteArrayConverter[String]= new StringType
    store.getIndex(random.nextString(10), baToString, baToString, BAComp, true).asInstanceOf[HGSortIndex[String,String]]
  }

  def getBidirectionalIndex(store:HGStore):HGBidirectionalIndex[String, String] = {
    val baToString: ByteArrayConverter[String]= new StringType
    val a = store.getBidirectionalIndex(random.nextString(10), baToString, baToString, BAComp, true)
    a
  }

  def getGraph(config:HGConfiguration):HyperGraph = {
    val graph = new HyperGraph()
    graph.setConfig(config)
    graph.open("/home/ingvar/bin/bje")
    graph
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


  def arraysEqual[T](left: Array[T], right: Array[T]): Boolean = {
    if (left.equals(right))  true
    else if (left == null || right == null)  false
    else if (left.length != right.length)  false
    else  ( left.deep ==   right.deep) && (left.corresponds(right)(_ == _))
  }

}
