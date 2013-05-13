package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.HGStoreImplementation
import org.hypergraphdb.{HGConfiguration, HGPersistentHandle}
import TestCommons._
import collection.JavaConversions._
import scala.util.Try

/**
 * User: Ingvar Bogdahn
 * Date: 21.04.13
 */
class StorageLinkTest ( val tested:HGStoreImplementation,
                        val testData:Map[HGPersistentHandle,Array[HGPersistentHandle]],
                        val async:Boolean,
                        override val keyPred :Boolean = random.nextBoolean(),
                        override val valPred :Boolean= random.nextBoolean())  extends HazelTest
{

  type Tested = HGStoreImplementation
  type Key = HGPersistentHandle
  type Value = Array[HGPersistentHandle]

  setupStorImp(tested)

  val removeDate = testData.filter(i => keyPred).map{ case (handle, handleArray) => (handle, handleArray.filter( j => valPred)) }

  val mutations:Seq[(String, Boolean, DataMap, DataMap => Unit)] = Seq(
    ("def store(handle : HGPersistentHandle, link : Array[HGPersistentHandle])", false, testData, (a:DataMap) => a.foreach{ case (handle, link) => tested.store(handle, link)}),
    ("def removeLink(handle : HGPersistentHandle)", true, testData, (a:DataMap) => a.foreach{ case (handle, link) => tested.removeLink(handle)})
  )

  val validations = Seq(
    mkValidtForAll[Tested,Data]("getLink", (store:Tested,data:Data)  => repeatUntil(store.getLink, data._1)(arraysEqual(_, data._2))._3),
    mkValidtForAll[Tested,Data]("containsLink", (store:Tested,data:Data)  => repeatUntil(store.containsLink,data._1)(_ == true)._3)
  )

  def remTestAgainstGen(a:Value, b:Value) = {val c = a.diff(b); if (c.isEmpty) None else Some(c)}
}
