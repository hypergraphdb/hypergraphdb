package storage

import org.scalatest._
import flatspec._
import matchers._
import org.hypergraphdb.storage.HGStoreImplementation

abstract class StorageTestBase extends AnyFlatSpec with should.Matchers 
                                          with OptionValues 
                                          with Inside 
                                          with Inspectors {
  def storeImplementationClass = {
    "org.hypergraphdb.storage.bje.BJEStorageImplementation"
  }                                              

  def createStore() = {
    info("Using storage implementation " + storeImplementationClass)
    Class.forName(storeImplementationClass).newInstance.asInstanceOf[HGStoreImplementation]
  }
}
