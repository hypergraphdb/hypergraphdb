package org.hypergraphdb.storage.hazelstore.testing

import org.hypergraphdb.storage.HGStoreImplementation
import TestCommons._
import org.hypergraphdb.HGPersistentHandle

/**
 * User: Ingvar Bogdahn
 * Date: 21.04.13
 */
class StoreLinkPropTests /*(val store:HGStoreImplementation) extends GeneratorDrivenPropertyChecks with ShouldMatchers{

  import Generators.Handles._
  import org.scalacheck.Gen.{Params, oneOf}
  type LinkData = (HGPersistentHandle,Array[HGPersistentHandle])
  type DataData = (HGPersistentHandle,Array[Byte])

  val links:Seq[LinkData] = mkHandles(dataSize).map(key => (key,mkHandles(random.nextInt(dataSize)).toArray))
  val linksArb: Arbitrary[LinkData] = Arbitrary(oneOf(links))
  links.foreach{case (key1, har) => store.store(key1, har)}
  Thread.sleep(syncTime)
  containsLinkTest(linksArb)
  //getLinkTest
  val KeepRemoveSeqsPair = links.partition(i => random.nextBoolean())
  KeepRemoveSeqsPair._2.foreach{case (key2, data) => store.removeLink(key2)}
  val removeArb: Arbitrary[LinkData] = Arbitrary(oneOf(KeepRemoveSeqsPair._2))
  val keepArb: Arbitrary[LinkData] = Arbitrary(oneOf(KeepRemoveSeqsPair._1))
  containsLinkTest(keepArb)
  notContainsLinkTest(removeArb)
  getLinkTest(keepArb)
  notGetLinkTest(removeArb)


  def containsLinkTest(arb:Arbitrary[LinkData])  = forAll { (data: LinkData ) =>   store.containsLink(data._1) } _
  def notContainsLinkTest(arb:Arbitrary[LinkData])  = forAll { data: LinkData  =>   ! store.containsLink(data._1) } _
  def getLinkTest (arb:Arbitrary[LinkData])      = forAll { data: LinkData  =>   arraysEqual(store.getLink(data._1), data._2)  } _
  def notGetLinkTest (arb:Arbitrary[LinkData])      = forAll { data: LinkData  =>  store.getLink(data._1) == null  } _
}
*/
