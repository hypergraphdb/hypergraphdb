package storage

import org.hypergraphdb.HGStore
import org.hypergraphdb.HGIndex
import org.hypergraphdb.storage.ByteArrayConverter
import org.scalatest.Status
import org.scalatest.Args
import org.scalatest.ConfigMap
import org.scalatest.FixtureTestSuite
import org.scalatest.flatspec.FixtureAnyFlatSpecLike
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.hypergraphdb.storage.ByteArrayConverter
import java.util.Comparator
import org.hypergraphdb.storage.BAtoHandle
import org.hypergraphdb.`type`.javaprimitive.StringType
import org.hypergraphdb.storage.BAtoBA
import scala.collection.mutable.ArrayBuffer
import collection.convert.ImplicitConversions._
import collection.convert.ImplicitConversionsToScala._
import scala.util.Using
import scala.collection.mutable
import scala.util.Random
import org.hypergraphdb.HGHandle
import org.hypergraphdb.HGPersistentHandle
import org.hypergraphdb.HGRandomAccessResult.GotoResult

class IndexTests extends FixtureAnyFlatSpec with StorageTestEnv {
  
  val store: HGStore = getStore()
    
  def generateData[Key, Value](keyGenerator: () => Key, 
                               valueGenerator: Key => Value,
                               maxKeys: Int,
                               duplicateFraction: Float,
                               maxDuplicates: Int): Map[Key, Iterable[Value]] = {    
    val result = mutable.Map[Key, Iterable[Value]]()
    for (i <- 1 to maxKeys) {
      val key = keyGenerator()
      val values = new ArrayBuffer[Value]()
      values += valueGenerator(key)
      if (random.nextFloat() < duplicateFraction) {
        val count = random.nextInt(maxDuplicates) + 1
        for (j <- 0 until count)
          values += valueGenerator(key)
      }
      result(key) = values
    }
    result.toMap
  }

  val uuids = generateData[HGPersistentHandle, HGPersistentHandle](() => newhandle(), _ => newhandle(), 500, 0.5, 10)

  val strings = Map[String, Iterable[String]](
    "hi" -> List("bye")
  )

  class Metadata[Data](val generator: () => Data, 
                       val converter: ByteArrayConverter[Data],
                       val comparator: Ordering[Data],
                       val dbcomparator: Comparator[Array[Byte]])

  class Fixture[Key, Value](val data: Map[Key, Iterable[Value]], val key: Metadata[Key], val value: Metadata[Value])

  type FixtureParam = Fixture[?, ?]

  protected override def runTests(testName: Option[String], args: Args): Status = {
    println("Running my way or the high way")    
    val uuidComparator = new Ordering[HGPersistentHandle] {
      def compare(left: HGPersistentHandle, right: HGPersistentHandle): Int = help.ByteArrayComparator.compare(left.toByteArray, right.toByteArray)
    }
    val uuidFixture = new Fixture(
      uuids, 
      Metadata[HGPersistentHandle](() => newhandle(), BAtoHandle.getInstance(store.getConfiguration.getHandleFactory), uuidComparator, null),
      Metadata[HGPersistentHandle](() => newhandle(), BAtoHandle.getInstance(store.getConfiguration.getHandleFactory), uuidComparator, null))
    val index = store.getIndex("theindex", 
                               uuidFixture.key.converter, 
                               uuidFixture.value.converter,
                               uuidFixture.key.dbcomparator, 
                               uuidFixture.value.dbcomparator, true)
    val status = super.runTests(testName, withNewConfig(args.configMap + 
        ("fixture" -> uuidFixture), args))
    if (!status.isCompleted())
      status
    else {
      val stringType = new StringType()
      val stringComparator = scala.math.Ordering.String
      val stringsFixture = new Fixture(
        strings, 
        Metadata[String](() => randomString(50), stringType, stringComparator, null),
        Metadata[String](() => randomString(50), stringType, stringComparator, null))
      store.removeIndex("theindex")
      val index = store.getIndex("theindex", 
                                 stringsFixture.key.converter, 
                                 stringsFixture.value.converter, 
                                 stringsFixture.key.dbcomparator, 
                                 stringsFixture.value.dbcomparator, 
                                 true)        
      super.runTests(testName, withNewConfig(args.configMap + 
        ("fixture" -> stringsFixture), args))
    }
  }

  override def withFixture(test: OneArgTest) = {
    try {      
      val fixture = test.configMap("fixture").asInstanceOf[FixtureParam]
      withFixture(test.toNoArgTest(fixture))
    }
    finally {

    }
  }

  it should "allow new entries to be added" in { (fixture: FixtureParam) =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    fixture.data.foreach( (key:Any, values:Iterable[Any]) => values.foreach(index.addEntry(key, _)))
    fixture.data.keySet.foreach( key => {
      assert(fixture.data(key).contains(index.findFirst(key)))
    })
  }

  it should "close, open, check if open properly" in { fixture => 
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    assert(index.isOpen())
    index.close()
    assert(!index.isOpen())
    assertThrows[Exception] { index.count() == fixture.data.size }
    assertThrows[Exception] { index.scanKeys() }
    index.open()
    assert(index.isOpen())
    val first = fixture.data.head
    assert(collect(index.find(first._1)).toSet == first._2.toSet)
    assert(index.stats().countKeys() == fixture.data.size)
  }

  it should "count properly" in { fixture =>
    val index = store.getIndex("countindex", BAtoBA.getInstance(), BAtoBA.getInstance(), null, null, true)
    try {
      assert(index.count() == 0)
      index.addEntry(super.randomBytes(14), super.randomBytes(54))
      assert(index.count() == 1)
      for (i <- 0 to 100)
        index.addEntry(super.randomBytes(i + 1), super.randomBytes(4*(i +2 )))
      assert(index.count() == 102)
      val keys = new ArrayBuffer[Array[Byte]]()
      Using.resource(index.scanKeys) { rs => rs.take(20).foreach(keys.add(_)) }
      keys.foreach(index.removeAllEntries)
      assert(index.count() == 102 - 20)
    }
    finally {
      store.removeIndex("countindex")
    }
  }  

  it should "return its name" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    assert(index.getName == "theindex")
  }

  it should "return correct result set for an existing key" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    val data = fixture.data
    // let's pick 10 keys at random from the map, by picking 10 random integers
    // and then doing a lookup
    val indices = List.tabulate(10)(n => random.nextInt(data.size))

    for ( (key, cound) <- data.keySet.zipWithIndex if indices.contains(cound)) {
      val values = data(key)
      Using.resource(index.find(key)) { rs => assert(values.forall(rs.goTo(_, true) == GotoResult.found)) }
    }
  }

  it should "return empty set for non existing key" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    Using.resource(index.find(fixture.key.generator())) { rs => assert(!rs.hasNext()) }
  }

  it should "return correct value counts for keys" in { fixture => 
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    fixture.data.keys.foreach(key => assert(index.count(key) == fixture.data(key).size))
    assert(index.count(fixture.key.generator()) == 0)
  }

  it should "scanKeys return all keys" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    assert(fixture.data.keySet == collect(index.scanKeys).toSet)
  }

  it should "scanValues return all Values" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")    
    assert(fixture.data.values.flatten.toSet == collect(index.scanValues).toSet)
  }  
}
