package org.hypergraphdb.storage.hazelstore.testing

import scala.util.Try
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._
import com.hazelcast.core.Hazelcast

/**
 * Author: Ingvar Bogdahn
 * Date: 19.04.13
 */
trait HazelTest {  type Tested
  type Key
  type Value
  type Data = (Key, Value)
  type DataMap = Map[Key, Value]

  // a test consists of a series of mutations on the state of the testSubject and a series of tests aka "validations" performed after each mutation.
  val async:Boolean
  val tested: Tested
  val testData:DataMap
  val testOf = "T E S T  O F"
  def remTestAgainstGen(a:Value, b:Value):Option[Value]

  val keyPred :Boolean = random.nextBoolean()
  val valPred :Boolean= random.nextBoolean()

  // a mutation is defined as
  // a description of the mutation,
  // a boolean indicating whether the inputData parameter is used to remove Data,
  // inputData
  // the function  testSubject + inputData => Unit (new state of testSubject)
  val mutations:Seq[(String, Boolean, DataMap, DataMap => Unit)]

  // a validation is defined as a description of the validation and function on a testSubjectState, each one testing a specific functionality
  val validations : Seq[(String, (Tested,Seq[Data]) => Try[Boolean])]


  lazy val a = mutations.zipWithIndex.map(i => i._2 -> i._1).toMap

  // Type: Iterable of: description + inputData + mutationFunction + Either: TestAgainstDataPresent(after adding) OR Pair: testIsPresent And testIsAbsent
  lazy val mutationSeqEither:  Iterable[(String, DataMap, (DataMap) => Unit, Either[DataMap, (Map[Key, Value], DataMap)])]=
    a.map (i => {
      val (index, (desc, removing, input, func)) :  (Int, (String, Boolean, DataMap, DataMap => Unit)) = i
      if(!removing)
        Some((desc, input, func, Left(input)))
      else
      {
        val previous = a(index-1)
        val prevInput = previous._3
        val testPresent = prevInput.map( i => {
          val (key, valSeq) : (Key, Value) = i
          val newValSeq : Option[Value]  = input.get(key).map(i => remTestAgainstGen(valSeq, i)).flatten
          if(!newValSeq.isDefined)
            None
          else
            Option((key,newValSeq.get))
        }).flatten.toMap

        val testAbsent = input

        Some((desc, input, func, Right(testPresent,testAbsent)))
      }}
    ).flatten


  def test = mutationSeqEither.map{case (mutationName, inputData, mutationFunc, testAgainstDataEither) =>
  {
    log("\n\nOn: " + tested.getClass.getSimpleName + s"\n\n N o w   a p p l y i n g   m u t a t i o n  :  $mutationName")
    mutationFunc(inputData)
    val results :  Seq[Either[(String, Try[Boolean]), ((String, Try[Boolean]), (String, Try[Boolean]))]] =
      validations.map { case (validationName,test) =>
      {
        log(s"Now testing $validationName on $tested")
        testAgainstDataEither
          .left.map(i => (validationName, test(tested,i.toSeq)))    // Mutation was an addition, so only checking if input data is present
          .right.map(j => Pair(// Mutation was a removal so two things to check:
          (validationName + ": Testing if undeleted Data still present", test(tested,j._1.toSeq)),   // Test not deleted data is still present
          (validationName + ": Testing if deleted Data was indeed deleted", test(tested,j._2.toSeq)))   // Test deleted data is not present
        )
      }}
    val failures = results.filter(k =>  k.left.map(_._2.isFailure).left.getOrElse(
      k.right.map(k => k._1._2.isFailure || ! k._2._2.isFailure).right.get))

    if(failures.size >0) println("\n\n\nF A I L U R E S: ")
    failures.foreach(i => println(i.left.getOrElse(i.right.get)))

    (mutationName,results)
  }
  }

  def run= {
    val a = timeMeasure(test)
    Hazelcast.shutdownAll()
    //    (tested.getClass.getCanonicalName , a._2, ( if (async) (a._1/1000 - mutations.size*syncTime*1000) else a._1/1000 ))
    log(s"finished test $this in " + a._1/1000000 + " nanos")
    (tested.getClass.getCanonicalName , a._2, a._1)
  }

}