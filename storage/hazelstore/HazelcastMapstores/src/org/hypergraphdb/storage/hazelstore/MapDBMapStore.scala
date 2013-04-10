package org.hypergraphdb.storage.hazelstore

import java.util
import org.mapdb._
import java.io.File
import com.hazelcast.core._
import collection.JavaConversions._
import java.util.Properties


class MapDBMapStore[K,V] (val file:String, val mapName:String, keyClass:Class[K], valClass:Class[V]) extends MapStore[K,V] with MapLoader[K,V] with MapLoaderLifecycleSupport{

  throw new Exception("MapDBMapStore not yet tested.")

  val db = DBMaker.newFileDB(new File(file))
    .closeOnJvmShutdown()
    .make();

  val map:java.util.Map[K,V] = db.getHashMap(mapName)

  def store(p1: K, p2: V) {map.put(p1,p2)}

  def storeAll(p1: util.Map[K, V]) {map.putAll(p1)}

  def delete(p1: K) {map.remove(p1)}

  def deleteAll(p1: util.Collection[K]) {p1.foreach(k => map.remove(k))}

  def load(p1: K) = map.get(p1)

  def loadAll(p1: util.Collection[K]): java.util.Map[K, V] = {
    val localMap = new java.util.HashMap[K, V](p1.size())
    p1.foreach(key => localMap.put(key, map.get(key)))
    localMap
  }   //p1.map[K,V](key => (key,map.get(key))).toMap  // some strange type error

  def loadAllKeys(): java.util.Set[K] = map.keySet()

  def init(p1: HazelcastInstance, p2: Properties, p3: String) {}

  def destroy() {}
}

class MapDBMapStoreFactory[K,V] extends MapStoreFactory[K,V]{
  def newMapStore(p1: String, p2: Properties) = {
    val file = p2.getProperty("file")
    val mapName = p2.getProperty("mapName")
    val keyClassName = p2.getProperty("keyClassName")
    val valClassName = p2.getProperty("valClassName")

    val keyClass: Class[_] = Class.forName(keyClassName)
    val valClass: Class[_] = Class.forName(valClassName)

    new MapDBMapStore(file, mapName,keyClass,valClass).asInstanceOf[MapLoader[K, V]]
  }
}