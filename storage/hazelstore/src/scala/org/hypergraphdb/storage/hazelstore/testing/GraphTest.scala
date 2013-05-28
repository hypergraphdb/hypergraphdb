package org.hypergraphdb.storage.hazelstore.testing

import collection.JavaConversions._
import org.hypergraphdb.{HGPlainLink, HGHandle, HyperGraph}
import org.hypergraphdb.storage.hazelstore.HazelStoreConfig
import org.hypergraphdb.HGQuery.hg._
import org.hypergraphdb.HGQuery.hg
import org.hypergraphdb.query.{ComparisonOperator, AtomPartCondition, AtomTypeCondition}
import TestCommons._


class GraphTest(graph:HyperGraph,bootstrap:Boolean = true)(implicit testDataSize:Int ) {
  import Generators.Strings._
  def au[T](t: T): HGHandle = assertAtom(graph, t)
  def ad[T](t: T): HGHandle = graph.add(t)
  def gh[T](t: T): HGHandle = graph.getHandle(t)


  def run:Long = timeMeasure{

    val sessionID = random.nextInt()
    val birthdayRange = 1800 to 2000
    // querying for absent type should not cause weird exceptions
    hg.getOne(graph,new AtomTypeCondition(classOf[Person]))

    (1 to dataSize).foreach(i => {
      graph.add(randomString)
    })


    //val links = (0 to 10).map(i => graph.add(new WhenThen(au(i), au(i + 1)))).toSeq
    val links = (0 to 10).map(i => graph.add(new HGPlainLink(au(i), au(i + 1)))).toSeq
    println("\n\n\nnow Adding Persons\n\n\n")

    val persons           = (1 to dataSize).map(i => new Person(sessionID, random.nextString(i),random.nextString(i),random.nextString(i), birthdayRange(i)))
    val addMeasure = timeMeasure(persons.map(p => graph.add(p)))
    println("adding took " +addMeasure._1)
    val personsHandles    = addMeasure._2
    val personsBack       = (0 to dataSize-1).map(i => graph.get(personsHandles(i)) == persons(i))

    assert(personsBack.forall(_ == true))



    val incid = (0 to testDataSize).map(i => graph.getIncidenceSet(au(i)))        // ok
    val linksBack = (0 to testDataSize).map(i => getAll(graph, link(gh(i))))     // ok
    //  assert(links.corresponds(linksBack)(_==_))

    val x = graph.find(apply(targetAt(graph, 1),orderedLink(anyHandle(), gh(2))))
    println("graph.find(apply(targetAt(graph, 1),orderedLink(anyHandle(), gh(2)))).hasNext:  " + x.hasNext + "\n")


    val bdMiddle = birthdayRange(dataSize/2)

    println("Testing persons birthday EQ/GT/GTE/LT/LTE than " + bdMiddle)

    def testEQGTGTELTLTE(fun: (Person,Int) => Boolean, comp:ComparisonOperator, againstBirthday: Seq[Int], againstPerson:Seq[Person] = persons) = againstBirthday.map(birthday => {
        val check = againstPerson.filter( person => fun(person, birthday)).toSet
        val back = getAll(graph, and(new AtomTypeCondition(classOf[Person]), new AtomPartCondition("ID".split("\\."),sessionID), new AtomPartCondition("birthyear".split("\\."), birthday, comp))).asInstanceOf[java.util.List[Person]]
      (back.forall(person => check(person)), check.size, back.size)
      })

    val allEqual = testEQGTGTELTLTE(_.getBirthyear == _, ComparisonOperator.EQ, birthdayRange)
    assert(allEqual.forall(pair => pair._1 & pair._2 == pair._3 ))

    val gt = testEQGTGTELTLTE(_.getBirthyear > _, ComparisonOperator.GT, Seq(bdMiddle))
    assert(gt.forall(pair => pair._1 & pair._2 == pair._3 && pair._2 > 0))

    val gte = testEQGTGTELTLTE(_.getBirthyear >= _, ComparisonOperator.GTE, Seq(bdMiddle))
    assert(gte.forall(pair => pair._1 & pair._2 == pair._3 && pair._2 > 0))

    val lt = testEQGTGTELTLTE(_.getBirthyear < _, ComparisonOperator.LT, Seq(bdMiddle))
    assert(lt.forall(pair => pair._1 & pair._2 == pair._3 && pair._2 > 0))

    val lte = testEQGTGTELTLTE(_.getBirthyear <= _, ComparisonOperator.LTE, Seq(bdMiddle))
    assert(lte.forall(pair => pair._1 & pair._2 == pair._3 && pair._2 > 0))


    //getAll(graph, and(new AtomTypeCondition(classOf[Person]), gt("birthyear", 5))).foreach(println)

    println("removing persons took " + timeMeasure(personsHandles.foreach(graph.remove)))

    val personsStillThere = hg.getAll(graph, new AtomTypeCondition(classOf[Person])).asInstanceOf[java.util.List[Person]].toSeq

    println("rechecking everyhing got deleted")
    val eqDel = testEQGTGTELTLTE(_.getBirthyear == _, ComparisonOperator.EQ, birthdayRange, personsStillThere)
    assert(eqDel.forall(pair => pair._2 == pair._3 && pair._2 == 0))

    val gtDel = testEQGTGTELTLTE(_.getBirthyear > _, ComparisonOperator.GT, Seq(bdMiddle),personsStillThere)
    assert(gtDel.forall(pair => pair._2 == pair._3 && pair._2 == 0))

    val gteDel = testEQGTGTELTLTE(_.getBirthyear >= _, ComparisonOperator.GTE, Seq(bdMiddle),personsStillThere)
    assert(gteDel.forall(pair => pair._2 == pair._3 && pair._2 == 0))

    val ltDel = testEQGTGTELTLTE(_.getBirthyear < _, ComparisonOperator.LT, Seq(bdMiddle),personsStillThere)
    assert(ltDel.forall(pair => pair._2 == pair._3 && pair._2 == 0))

    val lteDel = testEQGTGTELTLTE(_.getBirthyear <= _, ComparisonOperator.LTE, Seq(bdMiddle),personsStillThere)
    assert(lteDel.forall(pair => pair._2 == pair._3 && pair._2 == 0))


    graph.close
    //res._1
    Unit
  }._1


}
