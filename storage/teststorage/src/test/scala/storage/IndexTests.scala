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
import org.hypergraphdb.HGRandomAccessResult
import org.hypergraphdb.HGBidirectionalIndex
import org.scalatest.Tag
import org.hypergraphdb.HGSortIndex
import scala.collection.SortedMap
import org.hypergraphdb.handle.SequentialUUIDHandleFactory
import org.scalatest.SucceededStatus
import org.scalatest.Succeeded

object BiDirectional extends Tag("BiDirectional")
object SortIndex extends Tag("SortIndex")

class IndexTests extends FixtureAnyFlatSpec with StorageTestEnv {
  
  val store: HGStore = getStore()
    
  /**
   * Represents info about the data type in a data set. 
   * Whether working on keys or values, a particular test might need to know
   * how to generate new data items, how to convert them to byte buffers, compare them in
   * memory (in their natural data type form) or compare them as byte buffers as the storage would.
   */
  class Metadata[Data](val generator: () => Data, 
                       val converter: ByteArrayConverter[Data],
                       val comparator: Ordering[Any],
                       val dbcomparator: Comparator[Array[Byte]])

  val uuidComparator = new Ordering[Any] {
    def compare(left: Any, right: Any): Int = help.ByteArrayComparator.compare(
      left.asInstanceOf[HGPersistentHandle].toByteArray, right.asInstanceOf[HGPersistentHandle].toByteArray)
  }
  val aboutuuids = Metadata[HGPersistentHandle](
    () => newhandle(), 
    BAtoHandle.getInstance(store.getConfiguration.getHandleFactory), 
    uuidComparator, 
    null)

  val stringType = new StringType()
  val stringComparator = new Ordering[Any] {
    def compare(left: Any, right: Any): Int = 
      Ordering.String.compare(left.asInstanceOf[String], right.asInstanceOf[String])
  }
  val aboutstrings = Metadata[String](() => randomString(50), stringType, stringComparator, null)

  /**
    * @param keyGenerator Produces new keys on demand
    * @param valueGenerator Produces new values for a give key. The key can be ignored if
    * there is no logical dependency b/w values and keys.
    * @param maxKeys The maximum number of keys to generate
    * @param duplicateFraction The fraction of keys with more than one corresponding value
    * @param maxDuplicates The maximum number of values per key.
    */
  def generateData[Key, Value](aboutkeys: Metadata[Key], 
                               aboutvalues: Metadata[Value],
                               maxKeys: Int,
                               duplicateFraction: Float,
                               maxDuplicates: Int): SortedMap[Key, Iterable[Value]] = {    
    implicit val keyorder : Ordering[Key] = aboutkeys.comparator.asInstanceOf[Ordering[Key]]
    implicit val valueorder : Ordering[Value] = aboutvalues.comparator.asInstanceOf[Ordering[Value]]
    val result = mutable.SortedMap[Key, Iterable[Value]]()
    // make sure we have a few single valued keys    
    for (i <- 0 until 5) {
      val key = aboutkeys.generator()
      result(key) = List(aboutvalues.generator())
    }
    for (i <- 5 until maxKeys) {
      val key = aboutkeys.generator()
      val values = new ArrayBuffer[Value]()
      values += aboutvalues.generator()
      if (random.nextFloat() < duplicateFraction) {
        val count = random.nextInt(maxDuplicates) + 1
        for (j <- 0 until count)
          values += aboutvalues.generator()
      }
      result(key) = values
    }
    result(result.keySet.head) = List(aboutvalues.generator(), aboutvalues.generator())
    result
  }

  val uuids = generateData[HGPersistentHandle, HGPersistentHandle](aboutuuids, aboutuuids, 50, 0.5, 50)

  val strings = SortedMap[String, Iterable[String]](
    "hi" -> List("bye")
  )

  /**
   * Represents all the information need to run a this test suite. All tests are run on a fixture. 
   */
  class Fixture[Key, Value](val data: SortedMap[Key, Iterable[Value]], 
                            val key: Metadata[Key], 
                            val value: Metadata[Value])

  type FixtureParam = Fixture[?, ?]

  protected def runTestsWithFixture[Key, Value]
    (testName: Option[String], args: Args)(fixture: Fixture[Key, Value]): Status = {
    store.getIndex("theindex", 
          fixture.key.converter, 
          fixture.value.converter,
          fixture.key.dbcomparator, 
          fixture.value.dbcomparator, true)
    try {           
      super.runTests(testName, withNewConfig(args.configMap + ("fixture" -> fixture), args))
    }
    finally {
      store.removeIndex("theindex")
    }
  }

  protected override def runTests(testName: Option[String], args: Args): Status = {
    val fixtures = List(
      new Fixture(uuids, aboutuuids, aboutuuids),
      new Fixture(strings, aboutstrings, aboutstrings)
    )
    fixtures.map(fixture => {
      runTestsWithFixture(testName, args)(fixture)
    }).find(status => status.isCompleted()).getOrElse(SucceededStatus)
  }

  override def withFixture(test: OneArgTest) = {
    try {      
      val fixture = test.configMap("fixture").asInstanceOf[FixtureParam]
      info("Running test " + test.name + " with tags " + test.tags)
      withFixture(test.toNoArgTest(fixture))      
      // if (test.tags.contains(ToDebug.name))
      //   withFixture(test.toNoArgTest(fixture))
      // else
      //   Succeeded
    }
    finally {

    }
  }

  it should "allow new entries to be added"   in { (fixture: FixtureParam) =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")
    fixture.data.foreach( (key:Any, values:Iterable[Any]) => tx(values.foreach(index.addEntry(key, _))))
    fixture.data.keySet.foreach( key => {
      tx(collect(index.find(key)).toSet).map(
        storedSet => assert(storedSet == fixture.data(key).toSet))
    })
  }

  it should "scan only keys, not values" in { fixture =>
    val index: HGIndex[Any, Any] = store.getIndex("theindex")    
    tx(Using.resource(index.scanKeys()) { rs =>       
      assert(rs.forall(fixture.data.keySet.contains(_)))
      rs.goAfterLast()
      rs.prev()
      assert(fixture.data.keySet.contains(rs.current()))
      rs.goBeforeFirst()
      rs.next()
      assert(fixture.data.keySet.contains(rs.current()))
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

  it should "return correct estimated stats" in { fixture =>
    info("FIXME: THIS TEST IS A TODO")
  } 

  it should "return values with key in their natural order" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    val order = fixture.value.comparator    
    fixture.data.keys.foreach(key => {
      Using.resource(index.find(key)) ( left => {
        Using.resource(index.find(key)) ( right => {
          if (right.hasNext()) {
            right.next
            assert(left.zip(right).forall ( p =>  order.compare(p._1, p._2) < 0 ) )
          }
        })
      })
    })
  }

  // Check navigation of various result sets: next, prev, goTo...        
  it should "support result set navigation in both directions as well as random access" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    val marks = ArrayBuffer[Any]()
    val maxsteps = 1000
    var first: Option[Any] = None
    var last: Option[Any] = None
    val sortedkeys = fixture.data.keySet.toSeq.sorted(fixture.key.comparator)
    tx(Using.resource(index.scanKeys()) { rs =>
      if (rs.hasNext)
        first = Some(rs.next())
      assert(first.get == sortedkeys.head)
      for (i <- 1 until maxsteps if rs.hasNext()) {
        val key = rs.next()
        if (marks.size < 5 && random.nextInt(5) == 0) {                    
          marks.append(key)
        }
      }
      rs.goAfterLast
      assert(!rs.hasNext)
      assert(rs.hasPrev)
      last = Some(rs.prev())
      rs.goBeforeFirst()
      assert(!rs.hasPrev)
      assert(rs.hasNext)
      
      assert(last.get == sortedkeys.last)

      assert(rs.goTo(fixture.key.generator(), true) == GotoResult.nothing)

      marks.foreach(mark => {
        assert(rs.goTo(mark, true) == GotoResult.found)
        assert(rs.current() == mark)
        val i = sortedkeys.indexOf(mark)
        assert  (i > 0)
        assert(rs.hasPrev())
        assert(rs.prev() == sortedkeys(i - 1))
        rs.next()
        if (i < sortedkeys.size - 1) {
          assert(rs.hasNext())
          assert(rs.next() == sortedkeys.get(i + 1))
        }
      })
    })
  }

  it should "correctly find keys > to a give key" taggedAs(SortIndex) in { fixture =>
    val index: HGSortIndex[Any,Any] = store.getIndex("theindex").asInstanceOf[HGSortIndex[Any, Any]]
    val sortedkeys = fixture.data.keySet.toSeq.sorted(fixture.key.comparator)

    // edge case: 1st key
    sortedkeys.headOption.map(firstKey => 
      val valueSet = mutable.Set[Any]()
      sortedkeys.tail.map(key => valueSet.addAll(fixture.data(key)))    
      tx(Using.resource(index.findGT(firstKey)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    )

    // edge case last key
    sortedkeys.lastOption.map(lastKey => 
      tx(Using.resource(index.findGT(lastKey)) { rs => assert(!rs.hasNext()) } )
    )

    // normal case in middle     
    if (sortedkeys.size > 2) {
      val valueSet = mutable.Set[Any]()      
      sortedkeys.drop(sortedkeys.size / 2).tail.map(key => valueSet.addAll(fixture.data(key)))
      val key = sortedkeys.drop(sortedkeys.size / 2).head
      tx(Using.resource(index.findGT(key)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    }
  }

  it should "correctly find keys >= to a give key" in { fixture =>
    val index: HGSortIndex[Any,Any] = store.getIndex("theindex").asInstanceOf[HGSortIndex[Any, Any]]
    val sortedkeys = fixture.data.keySet.toSeq.sorted(fixture.key.comparator)

    // edge case: 1st key
    sortedkeys.headOption.map(firstKey => 
      val valueSet = mutable.Set[Any]()
      sortedkeys.map(key => valueSet.addAll(fixture.data(key)))
      tx(Using.resource(index.findGTE(firstKey)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    )

    // edge case last key
    sortedkeys.lastOption.map(lastKey => 
      tx(Using.resource(index.findGTE(lastKey)) { rs => 
        assert(fixture.data(lastKey).toSet == collect(rs).toSet)
      })
    )

    // normal case in middle 
    if (sortedkeys.size > 2) {
      val valueSet = mutable.Set[Any]()      
      sortedkeys.drop(sortedkeys.size / 2).map(key => valueSet.addAll(fixture.data(key)))
      val key = sortedkeys.drop(sortedkeys.size / 2).head
      tx(Using.resource(index.findGTE(key)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    }    
  }

  it should "correctly find keys < to a give key" in { fixture =>
    val index: HGSortIndex[Any,Any] = store.getIndex("theindex").asInstanceOf[HGSortIndex[Any, Any]]
    val sortedkeys = fixture.data.keySet.toSeq.sorted(fixture.key.comparator)

    // edge case: 1st key
    sortedkeys.headOption.map(firstKey => 
      tx(Using.resource(index.findLT(firstKey)) { rs => assert(!rs.hasNext()) })
    )

    // edge case last key
    sortedkeys.lastOption.map(lastKey => 
      val valueSet = mutable.Set[Any]()
      sortedkeys.dropRight(1).map(key => valueSet.addAll(fixture.data(key)))
      tx(Using.resource(index.findLT(lastKey)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    )

    // normal case in middle 
    if (sortedkeys.size > 2) {
      val valueSet = mutable.Set[Any]()      
      sortedkeys.dropRight(sortedkeys.size / 2).map(key => valueSet.addAll(fixture.data(key)))
      val key = sortedkeys.drop(sortedkeys.size / 2).head
      tx(Using.resource(index.findLT(key)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    }    
  }

  it should "correctly find keys <= to a give key" in { fixture =>
    val index: HGSortIndex[Any,Any] = store.getIndex("theindex").asInstanceOf[HGSortIndex[Any, Any]]
    val sortedkeys = fixture.data.keySet.toSeq.sorted(fixture.key.comparator)

    // edge case: 1st key
    sortedkeys.headOption.map(firstKey => 
      tx(Using.resource(index.findLTE(firstKey)) { rs => 
        assert(collect(rs).toSet == fixture.data(firstKey).toSet) 
      })
    )

    // edge case last key
    sortedkeys.lastOption.map(lastKey => 
      val valueSet = mutable.Set[Any]()
      sortedkeys.map(key => valueSet.addAll(fixture.data(key)))
      tx(Using.resource(index.findLTE(lastKey)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    )

    // normal case in middle 
    if (sortedkeys.size > 2) {
      val valueSet = mutable.Set[Any]()      
      sortedkeys.dropRight(sortedkeys.size / 2 - 1).map(key => valueSet.addAll(fixture.data(key)))
      val key = sortedkeys.drop(sortedkeys.size / 2).head
      tx(Using.resource(index.findLTE(key)) { rs => 
        val data = collect(rs).toSet
        assert(data == valueSet)
      })
    }
  }

  // Check removals

  it should "remove a single entry for a single value per key" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    fixture.data.keys.find( key => fixture.data(key).size == 1).map(key => tx({
      val value = fixture.data(key).head
      index.removeEntry(key, value)
      Using.resource(index.find(key)) { rs => assert(!rs.hasNext()) }
    }))
  }

  it should "remove a single entry for a multiple values per key" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")    
    fixture.data.keys.find( key => fixture.data(key).size > 1).map(key => tx({
      val value = fixture.data(key).head
      val remaining = fixture.data(key).tail.head
      index.removeEntry(key, value)
      Using.resource(index.find(key)) { rs =>
        assert(rs.goTo(remaining, true) == GotoResult.found )
      }
    }))
  }

  it should "remove all entries for a key with single value" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    fixture.data.keys.find( key => fixture.data(key).size == 1).map(key => tx({
      val value = fixture.data(key).head
      index.removeAllEntries(key)
      Using.resource(index.find(key)) { rs => assert(!rs.hasNext()) }
    }))
  }  

  it should "remove all entries for a key with multiple values" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    fixture.data.keys.find( key => fixture.data(key).size > 1).map(key => tx({
      val value = fixture.data(key).head
      index.removeAllEntries(key)
      Using.resource(index.find(key)) { rs => assert(!rs.hasNext()) }
    }))    
  }

  it should "ignore removal of all entries for a key with no values (a NOP)" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    tx(index.removeAllEntries(fixture.key.generator()))
  }    

  it should "ignore removal of non-existing key-value entry when neither key nor value exist" in { fixture =>
    val index: HGIndex[Any,Any] = store.getIndex("theindex")
    val key = fixture.key.generator()
    tx(index.removeEntry(key, fixture.value.generator()))
  }
}
