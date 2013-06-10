package org.hypergraphdb.storage.hazelstore

import beans.BeanProperty
import com.hazelcast.config.{MapConfig, Config, NearCacheConfig}
import com.hazelcast.core.{HazelcastInstance, Hazelcast}



class HazelStoreConfig extends Serializable{
        var hazelcastConfig: Config = new Config
        var timeoutMillis:Int = 5000
        var useHCIndexing:Boolean = false
        var async: Boolean = false
        var useTransactionalCallables: Boolean = false
        var transactionalRetryCount: Int = 50
        var hazelMapConfigMap:Map[String, MapConfig] = Map.empty[String, MapConfig]

  def setHazelcastConfig(hc:Config)                     = { hazelcastConfig = hc; this}
  def setTimeoutMillis(to:Int)                          = { timeoutMillis = to; this}
  def setUseHCIndexing(i:Boolean)                       = { useHCIndexing = i; this}
  def setAsync(i:Boolean)                               = { async = i; this}
  def setUseTransactionalCallables(i:Boolean)           = { useTransactionalCallables= i; this}
  def setTransactionalRetryCount(i:Int)                 = { transactionalRetryCount= i; this}
  def setHazelMapConfigMap(map: Map[String, MapConfig]) = {hazelMapConfigMap = map; this}
  def addHazelMapConfigMapping(map: (String, MapConfig))= {hazelMapConfigMap = hazelMapConfigMap.updated(map._1, map._2) ; this}

  override def toString = s"Hazelstore-Config. Rarameters: async $async, useTransactionalCallables: $useTransactionalCallables, timeoutMillis: $timeoutMillis, useHCIndexing: $useHCIndexing"
}
