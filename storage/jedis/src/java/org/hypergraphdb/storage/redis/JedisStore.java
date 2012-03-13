package org.hypergraphdb.storage.redis;

import org.hypergraphdb.storage.HGStoreImplementation;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.Set;

public interface JedisStore extends HGStoreImplementation {
    byte[] INDICES = "indices".getBytes();

    Pool<Jedis> getWritePool();

    int lookupIndexID(String DBname)     // TODO - needs testing!!
    ;

    Integer lookupIndexIdElseCreate(String dbName)     // TODO - needs checking!!
    ;

    Pool<Jedis> getReadPool();

    Jedis getReadJedis();

    void returnReadJedis(Jedis j);

    void returnWriteJedis(Jedis j);

    void setUseCache(boolean useCache);

    // Jedis Utility methods.
    Set<byte[]> zrange(int jedisDBiD, byte[] key, int i, int i1);

    byte[] zrangeAt(int jedisDBiD, byte[] key, int i);

    Long zcard(int jedisDBiD, byte[] key);

    Long zrank(int jedisDBiD, byte[] key, byte[] value);

    Set<byte[]> keys(int jedisDbId, byte[] arg);

    int getRankOrInsertionPoint(int dbId, byte[] key, byte[] value);

    byte[] keySet(int jedisDbId);
}
