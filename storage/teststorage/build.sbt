name := "HyperGraphDB Storage Test Suite"
version := "0.1"
scalaVersion := "3.1.0"

val hgdbVersion = "2.0-SNAPSHOT"

// crossPaths := false

resolvers +=
  ("HyperGraphDB Artifacts" at "http://hypergraphdb.org/maven").withAllowInsecureProtocol(true)
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

libraryDependencies ++= Seq(
  "org.hypergraphdb" % "hypergraphdb" % hgdbVersion % Test
)

libraryDependencies += "com.github.sbt" % "junit-interface" % "0.13.2" % Test

libraryDependencies += "junit" % "junit" % "4.12" % Test

libraryDependencies += "org.hypergraphdb" % "hgdbtest" % hgdbVersion % Test

// BDB Java Edition
// libraryDependencies ++= Seq(
//   "com.sleepycat" % "je" % "5.0.104" changing()
// )
// libraryDependencies ++= Seq(
//   "org.hypergraphdb" % "hgbdbje" % hgdbVersion % Test
// )

// LMDB 
libraryDependencies ++= Seq(
  "org.hypergraphdb" % "hglmdb" % hgdbVersion % Test
)
libraryDependencies ++= Seq(
  "org.hypergraphdb" % "hgrocksdb" % hgdbVersion % Test
)

initialize ~= { _ =>
  System.setProperty( "org.hypergraphdb.storage.HGStoreImplementation", "org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB" )
}
