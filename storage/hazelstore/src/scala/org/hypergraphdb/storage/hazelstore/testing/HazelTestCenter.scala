package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.hazelstore.{HazelIndex12, Hazelstore5, HazelStoreConfig}
import scala.util.{Success, Try, Failure}

/**
 * User: Ingvar Bogdahn
 * Date: 19.04.13
 */
object HazelTestCenter{

def main (args: Array[String] ) {


  // CONFIG PERMUTATORS
  import BasicTests.configPermutations

  // DATA GENERATORS
  import Generators._
  import Handles._
  import Strings._
  import TestCommons._

  // TEST INSTANTIATION
  val tests :  Seq[(HazelStoreConfig, Seq[(String, Seq[(String, Seq[(String, Try[Boolean])])], Long)])] =
    configPermutations.map( c => {println("\n\nTESTING CONFIGURATION: " + c + "\n\n"); (c,Seq(
    new IndexHazelTest[String,String](getIndex(getStore(getConfig(c))),genStriLiMap().toSeq,c.async).run
//    new StorageDataTest(new Hazelstore5(c),(0 to dataSize).map(i => mkHandleBArrayPair),c.async).run,
//    new StorageLinkTest(new Hazelstore5(c),(0 to dataSize).map(i => mkHandleHandleSeqPair),c.async).run
  ))})

  println("\n\nF I N I S H E D    T E S T S\n\n")
  println("\nRESULTS:\n")

/*  val results: Seq[(HazelStoreConfig,Boolean, Seq[(String, Long,Seq[(String, Seq[(String, Option[Throwable])])])])] = tests.map( config =>
//val results = tests.map( config =>
    config._2 match {
      case Seq((componentTestName, Seq((mutuationName, Seq((validationName, Failure(failure))))), benchmark)) => (config._1, false, Seq((componentTestName, benchmark, Seq((mutuationName, Seq((validationName, Some(failure))))))))
      case Seq((componentTestName, Seq((mutuationName, Seq((validationName, Success(_))))), benchmark))       => (config._1, true,  Seq((componentTestName, benchmark, Seq((mutuationName, Seq((validationName, None)))))))

    }
  )
  results.foreach( i => {
    i._2 match {
      case false => {
        println(s"Failure while testing config " + i._1)
        i._3.foreach( j => {
          println("testing " + j._1)
          j._3.foreach( k => {
            println(k._1)
            k._2.foreach( l => {
              println(l._1)
              l._2.foreach( m => m.printStackTrace())
            })
          })
        })
      }
    }
  })
  */
}
}
