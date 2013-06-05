package org.hypergraphdb.storage.hazelstore.testing

object Server {
  def main(args: Array[String]) {
    import com.hazelcast.core._
    Hazelcast.getDefaultInstance
  }
}
