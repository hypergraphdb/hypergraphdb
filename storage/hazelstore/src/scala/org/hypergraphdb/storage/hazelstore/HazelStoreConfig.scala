package org.hypergraphdb.storage.hazelstore

import beans.BeanProperty
import com.hazelcast.config.{Config, NearCacheConfig}
import com.hazelcast.core.{HazelcastInstance, Hazelcast}


class HazelStoreConfig{
                        @BeanProperty var hazelConfig: Config = new Config
                        @BeanProperty var timeoutMillis:Int = 200
                        @BeanProperty var useHCIndexing:Boolean = false

  //@BeanProperty var hazelcastInstance:Option[HazelcastInstance] = Some(Hazelcast.newHazelcastInstance())
//                        @BeanProperty var dataDBNearCacheConfig:NearCacheConfig = new NearCacheConfig() // TODO - make use NearCache...
//                        @BeanProperty var linkDBNearCacheConfig:NearCacheConfig = new NearCacheConfig() // TODO - make use NearCache...
                        @BeanProperty var async: Boolean = false  // only active if transactional == false
}
