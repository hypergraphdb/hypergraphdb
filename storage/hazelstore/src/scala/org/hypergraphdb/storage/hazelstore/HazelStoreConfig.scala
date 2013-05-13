package org.hypergraphdb.storage.hazelstore

import beans.BeanProperty
import com.hazelcast.config.{Config, NearCacheConfig}
import com.hazelcast.core.{HazelcastInstance, Hazelcast}


class HazelStoreConfig extends Serializable{
        var hazelcastConfig: Config = new Config
        var timeoutMillis:Int = 5000
        var useHCIndexing:Boolean = false
        var async: Boolean = false
        var useTransactionalCallables: Boolean = false
        var transactionalRetryCount: Int = 50

  def setHazelcastConfig(hc:Config):HazelStoreConfig = { hazelcastConfig = hc; this}
  def setTimeoutMillis(to:Int):HazelStoreConfig = { timeoutMillis = to; this}
  def setUseHCIndexing(i:Boolean):HazelStoreConfig = { useHCIndexing = i; this}
  def setAsync(i:Boolean):HazelStoreConfig = { async = i; this}
  def setUseTransactionalCallables(i:Boolean):HazelStoreConfig = { useTransactionalCallables= i; this}
  def setTransactionalRetryCount(i:Int):HazelStoreConfig = { transactionalRetryCount= i; this}

  override def toString = s"Hazelstore-Config. Rarameters: async $async, useTransactionalCallables: $useTransactionalCallables, timeoutMillis: $timeoutMillis, useHCIndexing: $useHCIndexing"
}
