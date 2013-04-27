package org.hypergraphdb.storage.hazelstore.testing

import collection.JavaConversions._
import org.hypergraphdb.{HGPlainLink, HGHandle, HyperGraph}
import org.hypergraphdb.storage.hazelstore.HazelStoreConfig
import org.hypergraphdb.HGQuery.hg._
import org.hypergraphdb.HGQuery.hg
import org.hypergraphdb.query.{AtomPartCondition, AtomTypeCondition}
import TestCommons._


class GraphTest(graph:HyperGraph,bootstrap:Boolean = true)(implicit testDataSize:Int ) {
  import Generators.Strings._
  def au[T](t: T): HGHandle = assertAtom(graph, t)
  def ad[T](t: T): HGHandle = graph.add(t)
  def gh[T](t: T): HGHandle = graph.getHandle(t)


  def run:Long = {
    // querying for absent type should not cause weird exceptions
    hg.getOne(graph,new AtomTypeCondition(classOf[Person]))

    (1 to dataSize).foreach(i => {
      graph.add(randomString)
    })


    //val links = (0 to 10).map(i => graph.add(new WhenThen(au(i), au(i + 1)))).toSeq
    val links = (0 to 10).map(i => graph.add(new HGPlainLink(au(i), au(i + 1)))).toSeq
    println("\n\n\nnow Adding Persons\n\n\n")

    val persons           = (1 to dataSize).map(i => new Person(random.nextString(i),random.nextString(i),random.nextString(i),i))
    val personsHandles    = persons.map(p => graph.add(p))
    val personsBack       = (0 to dataSize-1).map(i => graph.get(personsHandles(i)) == persons(i))

    assert(personsBack.forall(_ == true))



    val incid = (0 to testDataSize).map(i => graph.getIncidenceSet(au(i)))        // ok
    val linksBack = (0 to testDataSize).map(i => getAll(graph, link(gh(i))))     // ok
    //  assert(links.corresponds(linksBack)(_==_))

    val x = graph.find(apply(targetAt(graph, 1),orderedLink(anyHandle(), gh(2))))

    println(" personsBackByINsuranceID 1 to 7")
    val pID7 = (1 to testDataSize).map(i => getAll(graph, and(new AtomTypeCondition(classOf[Person]), new AtomPartCondition("insurance".split("\\."), i))))
    pID7.foreach(println)

    println(" personsBackByINsuranceID gt than 5")
    getAll(graph, and(new AtomTypeCondition(classOf[Person]), gt("insurance", 5))).foreach(println)

    println("timeMeasure((0 to testDataSize).map(i => graph.add(random.nextString(i))))")
    val res = timeMeasure((0 to testDataSize).map(i => graph.add(random.nextString(i))))
    println(" took: " + res._1 + "\n")
    graph.close
    res._1
  }


}
