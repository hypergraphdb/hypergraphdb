package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.HGStoreImplementation
import org.hypergraphdb.HGPersistentHandle
import org.hypergraphdb.storage.hazelstore.testing.TestCommons._

/**
 * User: Ingvar Bogdahn
 * Date: 21.04.13
 */
class StoreIncidenceSetTest ( val tested:HGStoreImplementation,
                              val testData:Seq[(HGPersistentHandle,Array[HGPersistentHandle])],
                              val async:Boolean,
                              override val keyPred :Boolean = random.nextBoolean(),
                              override val valPred :Boolean= random.nextBoolean())  extends HazelTest
{
  type Tested = HGStoreImplementation
  type Data = (HGPersistentHandle,Array[HGPersistentHandle])
  val mutations = Seq.empty
  val validations = Seq.empty

  /*
  def getIncidenceResultSet(handle: HGPersistentHandle): HGRandomAccessResult[HGPersistentHandle]
  def getIncidenceSetCardinality(handle: HGPersistentHandle): Long
  */

  /*
  def removeIncidenceSet(handle: HGPersistentHandle)
  def addIncidenceLink(handle: HGPersistentHandle, newLink: HGPersistentHandle)
  def removeIncidenceLink(handle: HGPersistentHandle, oldLink: HGPersistentHandle)
  */
}
