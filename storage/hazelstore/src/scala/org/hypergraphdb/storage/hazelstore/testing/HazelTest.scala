package org.hypergraphdb.storage.hazelstore.testing

import scala.util.Try
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._
import com.hazelcast.core.Hazelcast

/**
 * Author: Ingvar Bogdahn
 * Date: 19.04.13
 */
trait HazelTest {
  type Tested
  type Data
  // a test consists of a series of mutations on the state of the testSubject and a series of tests aka "validations" performed after each mutation.
  val async:Boolean
  val tested: Tested
  val testData:Seq[Data]
  val testOf = "T E S T  O F"

  val keyPred :Boolean = random.nextBoolean()
  val valPred :Boolean= random.nextBoolean()

  // a mutation is defined as a description of the mutation, testData and a function of testSubject + TestData => the testSubject in a new state
  val mutations:Seq[(String, Seq[Data], Seq[Data] => Unit)]

  // a validation is defined as a description of the validation and function on a testSubjectState, each one testing a specific functionality
  val validations : Seq[(String, (Tested,Seq[Data]) => Try[Boolean])]

  def test = mutations.map
  {
    case( mutationName, testDataSeq, mutation)
    =>  {
          log("\n\nOn: " + tested.getClass.getCanonicalName + s"\n\n N o w   a p p l y i n g   m u t a t i o n  :  $mutationName")
//          testDataSeq.foreach(testData => mutation(testData))
          mutation(testDataSeq)
  //        log(s"waiting $syncTime millis for synchronization")
//          Thread.sleep(syncTime)

          val results = validations.map {
            case (validationName,test) => {
              log(s"Now testing $validationName on $tested");
              (validationName, test(tested,testDataSeq))
            }
          }
      val failures = results.filter(_._2.isFailure)
      if(failures.size >0) println("\n\n\nF A I L U R E S: ")
        failures.foreach(i => { println(i._1 + i._2.toString)})

          (mutationName,results)
        }
  }

  def run:(String, Seq[(String, Seq[(String, Try[Boolean])])],Long) = {
    val a = timeMeasure(test)
    Hazelcast.shutdownAll()
//    (tested.getClass.getCanonicalName , a._2, ( if (async) (a._1/1000 - mutations.size*syncTime*1000) else a._1/1000 ))
    log(s"finished test $this in " + a._1/1000000 + " nanos")
    (tested.getClass.getCanonicalName , a._2, a._1)
  }


}
