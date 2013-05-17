package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.hazelstore.Hazelstore


/**
 * User: Ingvar Bogdahn
 * Date: 19.04.13
 */
object HazelTestCenter{

def main (args: Array[String] ) {


  // CONFIG PERMUTATORS
  import TestCommons._

  // DATA GENERATORS
  import Generators._
  import Handles._
  import Strings._
  import TestCommons._

  // TEST INSTANTIATION
  val tests =
    configPermutations.map( c => {println("\n\nTESTING CONFIGURATION: " + c + "\n\n"); (c,Seq(
      new IndexHazelTest[String,String](getIndex(getStore(getHGConfig(c))),genStriLiMap(),c.async).run,
      new StorageDataTest(new Hazelstore(c),(0 to dataSize).map(i => mkHandleBArrayPair).toMap,c.async).run,
      new StorageLinkTest(new Hazelstore(c),(0 to dataSize).map(i => mkHandleHandleSeqPair).toMap,c.async).run
    ))})

  println("\n\nF I N I S H E D    T E S T S\n\n")
  println("\nRESULTS:\n")       /*println("\nRESULTS:\n")

  val partitioned = tests.map(_._2.map(_._2).flatten.map(_._2).flatten.partition{i:(String, Try[Boolean]) => i._2.isFailure})
  partitioned

  //def filterResults = (discr: Try[Boolean] => Boolean) => tests.map(i => (i, i._2.map(_._2).flatten.map(_._2).flatten.filter{i:(String, Try[Boolean]) => discr(i._2)}))
  def filterResults = (discr: Try[Boolean] => Boolean) => tests.map(i => (i._1,i._2.map(j => (j,j._3,j._2.map(k => (k._1, k._2.filter{i:(String, Try[Boolean]) => discr(i._2)}))))))
  val failures  = filterResults(_.isFailure)
  failures
  val success   = filterResults(_.isSuccess)
  success
    */

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
