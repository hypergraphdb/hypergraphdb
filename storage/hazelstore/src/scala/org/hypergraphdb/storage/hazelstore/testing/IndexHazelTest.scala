package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.{HGRandomAccessResult, HGSearchResult, HGSortIndex, HGIndex}
import scala.util.Try
import TestCommons.random
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import collection.JavaConversions._

/**
 * User: Ingvar Bogdahn
 * Date: 19.04.13
 */
class IndexHazelTest[K:Ordering,V](val tested:HGSortIndex[K,V],
                                   val testData:Map[K,Seq[V]],
                                   val async:Boolean,
                                   val keyFilter: Boolean = random.nextBoolean,
                                   val valFilter: Boolean = random.nextBoolean ) extends HazelTest
{
  type Tested = HGSortIndex[K,V]
  type Key = K
  type Value = Seq[V]
  import TestCommons._

  val removeData = testData.filter( i => keyFilter).map{ case (k,valSeq) => (k,valSeq.filter(ii => valFilter))}

  val mutations:Seq[(String, Boolean, DataMap, DataMap => Unit)] = Seq(
    ("addEntry", false, testData, ((dataSeq:DataMap) => dataSeq.foreach{ case (key, valueList) => valueList.foreach(tested.addEntry(key, _))})),
    ("removeEntry",  true, removeData, ((dataSeq:DataMap)=> dataSeq.foreach{ case (key, valueList) => valueList.foreach(tested.removeEntry(key, _))})),
    ("removeAllEntries",true, testData, ((dataSeq:DataMap)=> dataSeq.foreach{ case (key, _) => tested.removeAllEntries(key)}))
  )

  mutations

//String : description of tested function
// Tested = Index tested
// Seq[Data] = Testdata to test against
  val validations : Seq[(String, (Tested,Seq[Data]) => Try[Boolean])]=
    Seq(
      mkValidtForAll("def findFirst(key: K)", (index:Tested, data:Data) => {
        repeatUntil(index.findFirst,data._1)(data._2.contains(_))._3
      }),

      mkValidtForAll("def find(key: K)", (index:Tested, data:Data) => {
        repeatUntil(index.find, data._1)((a:HGRandomAccessResult[V]) => data._2.forall(v => a.goTo(v, true).equals(GotoResult.found)))._3
      }),

      mkValidt("def scanKeys()", (index:Tested, dataSeq:Seq[Data]) => {
        repeatUntil1(index.scanKeys)((a:HGRandomAccessResult[K]) => dataSeq.forall(key => a.goTo(key._1, true).equals(GotoResult.found)))._3
      }),

      mkValidt("def scanValues()()", (index:Tested, dataSeq:Seq[(K,Seq[V])]) =>
      {
        repeatUntil1(index.scanValues){  (a:HGRandomAccessResult[V]) =>
          val b = dataSeq.map(_._2).flatten
          b.forall(c => a.goTo(c, true).equals(GotoResult.found))
        }._3
      }),

      mkValidtForAll("def count(key: K)",(index:Tested,data:Data) => repeatUntil(index.count,data._1)(_ == data._2.size.toLong)._3),

      mkValidt[Tested,Data]("def count()",(index:Tested,data:Seq[Data]) => repeatUntil1(index.count)(_ == data.size.toLong)._3),

      findX("def findLTE(key: K)",_.findLTE(_),_.lteq(_,_)),
      findX("def findLT(key: K)",_.findLT(_),_.lt(_,_)),
      findX("def findGT(key: K)",_.findGT(_),_.gt(_,_)),
      findX("def findGTE(key: K)",_.findGTE(_),_.gteq(_,_))
    )

  // this function creates entries for the sequence of validations; for the findGT/GTE/LT/LTE methods
  // as such it creates a tuple of descriptor string and a test function
  // the test function, as above, takes the tested component and test input data and returns a success or failure.
  def findX(name:String, fun: (HGSortIndex[K,V], K) => HGSearchResult[V], disc:(Ordering[K],K,K) => Boolean) : (String, (Tested, scala.Seq[(K, scala.Seq[V])]) => Try[Boolean]) =
    (name, (index:Tested, dataSeq:Seq[(K,Seq[V])]) => Try
    {
      val allTrue = dataSeq.forall{ case (key, valueSeq)  =>
      {
        repeatUntil1(() => fun(index, key)){(b :HGSearchResult[V]) =>
          val ordering =  implicitly[Ordering[K]]
          val c = dataSeq.filter( pair =>  disc(ordering, pair._1, key)).map(_._1).toSet
          b.forall(key => c.contains(key))
        }._3
      }
      }
      assert(allTrue, name  +" failed")
      println("asserted in findX")
      allTrue
    }
      )


  def remTestAgainstGen(a:Value, b:Value) = {val c = a.diff(b); if (c.isEmpty) None else Some(c)}
}
/*
  val validatations : Seq[(String, (S,Seq[(K,Seq[V])]) => Try[Boolean])]=
   Seq(
     //    def findFirst(key: K)
      ("def findFirst(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
          {
            val allTrue = dataSeq.forall
              {
                case (key: K, values: Seq[V]) =>
                {
                  val a = index.findFirst(key)
                  values.contains(a)
                }
              }
            assert(allTrue, "findFirst failed")
            allTrue
          }),

     ("def find(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall
       {
         case (key: K, values: Seq[V]) =>
         {
           val a = index.find(key)
            values.forall(v => a.goTo(v, true).equals(GotoResult.found))
         }
       }
       assert(allTrue, "find failed")
       allTrue
     }),
     ("def scanKeys()", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val a = index.scanKeys()
       val allTrue = dataSeq.forall(key => a.goTo(key._1, true).equals(GotoResult.found))
       assert(allTrue, "scanKeys failed")
       allTrue
     }),
     ("def scanValues()()", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val a = index.scanValues()
       val b = dataSeq.map(_._2).flatten
       val allTrue = b.forall(c => a.goTo(c, true).equals(GotoResult.found))
       assert(allTrue, "scanValues failed")
       allTrue
     }),
     ("def count()", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val a = index.count
       val allTrue = a == dataSeq.size.toLong
       assert(allTrue, "count failed")
       allTrue
     }),
     ("def count(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall(b => index.count(b._1) == b._2.size.toLong)
       assert(allTrue, "count(key: K) failed")
       allTrue
     }),       ("def findLT(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall{ case (key, valueSeq)  =>
       {
         val b = index.findLT(key)
         val ordering =  implicitly[Ordering[K]]
         val c = dataSeq.filter( pair =>  ordering.lt(pair._1, key)).map(_._1).toSet
         b.forall(key => c.contains(key))
       }
       }
       assert(allTrue, "findLT failed")
       allTrue
     }),
     ("def findGT(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall{ case (key, valueSeq)  =>
       {
         val b = index.findGT(key)
         val ordering =  implicitly[Ordering[K]]
         val c = dataSeq.filter( pair =>  ordering.gt(pair._1, key)).map(_._1).toSet
         b.forall(key => c.contains(key))
       }
       }
       assert(allTrue, "findGT failed")
       allTrue
     }),
     ("def findLTE(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall{ case (key, valueSeq)  =>
       {
         val b = index.findLTE(key)
         val ordering =  implicitly[Ordering[K]]
         val c = dataSeq.filter( pair =>  ordering.lteq(pair._1, key)).map(_._1).toSet
         b.forall(key => c.contains(key))
       }
       }
       assert(allTrue, "findLTE failed")
       allTrue
     }),
     ("def findGTE(key: K)", (index:S, dataSeq:Seq[(K,Seq[V])]) => Try
     {
       val allTrue = dataSeq.forall{ case (key, valueSeq)  =>
       {
         val b = index.findGTE(key)
         val ordering =  implicitly[Ordering[K]]
         val c = dataSeq.filter( pair =>  ordering.gteq(pair._1, key)).map(_._1).toSet
         b.forall(key => c.contains(key))
       }
       }
       assert(allTrue, "findGTE failed")
       allTrue
     })
   )
*/