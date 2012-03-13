package org.hypergraphdb.storage.redis;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGConverter;
import org.hypergraphdb.transaction.HGTransactionManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

public class JedisBidirectionalIndex<KeyType, ValueType> extends JedisIndex<KeyType, ValueType> implements HGBidirectionalIndex<KeyType, ValueType>
{
    public JedisBidirectionalIndex(String indexName,
                                   JedisStore storage,
                                   HGTransactionManager transactionManager,
                                   ByteArrayConverter<KeyType> keyConverter,
                                   ByteArrayConverter<ValueType> valueConverter,
                                   Comparator comparator)
    {
        super(indexName, storage, transactionManager, keyConverter, valueConverter, comparator);
    }

    private List<byte[]> findByValues(ValueType value) {
        byte[] valueBA = valueConverter.toByteArray(value);
        updateKeySetAndIndexID();

        //Set<byte[]> resultKeySet = new LinkedHashSet<byte[]>(); //new TreeSet<byte[]>(new ByteArrayComparator());
        HashMap<byte[],Response<Long>> foundValueResponseMap = new HashMap<byte[], Response<Long>>(indexKeySet.size());
        HashMap<byte[],Boolean> foundValueMap = new HashMap<byte[], Boolean>(indexKeySet.size());
        List<byte[]> tempResultKeySet = new LinkedList<byte[]>();

        Jedis j = null;
        try {
            j = storage.getReadJedis();
            Pipeline pl = j.pipelined();
            pl.select(jedisDbId);
            for(byte[] key : indexKeySet.values()) {
                    foundValueResponseMap.put(key, pl.zrank(key, valueBA));
            }
            pl.sync();
            } 
        catch (Exception e) { } 
        finally { storage.returnReadJedis(j); }

        for(Map.Entry<byte[], Response<Long>> entry: foundValueResponseMap.entrySet())
            foundValueMap.put(entry.getKey(), entry.getValue().get()!=null);

        for(Map.Entry<byte[], Boolean> entry : foundValueMap.entrySet()){
            if(entry.getValue() == true)
                tempResultKeySet.add(entry.getKey());
        }
    return tempResultKeySet;
    }

    public HGRandomAccessResult<KeyType> findByValue(ValueType value) {

        List<byte[]> tempResultKeySet = findByValues(value);

        if(tempResultKeySet.size()==0)
            return (HGRandomAccessResult<KeyType>) HGRandomAccessResult.EMPTY;
        else{ 
            byte[] keySetContext = HGConverter.concat("findByValue".getBytes(), valueConverter.toByteArray(value));
            return new JKeyResultSet<KeyType>(storage, jedisDbId, new HashSet<byte[]>(tempResultKeySet), keyConverter, false);
//        return new JKeyResultSetOverValue<KeyType, ValueType>(storage, jedisDbId, resultKeySet, keyConverter, valueConverter, false);
        }
    }

    public KeyType findFirstByValue(ValueType value) {
        List<byte[]> tempResultKeySet = findByValues(value);
        return keyConverter.fromByteArray(tempResultKeySet.iterator().next());
    }

    public long countKeys(ValueType value) {
        List<byte[]> tempResultKeySet = findByValues(value);
        return tempResultKeySet.size();
    }
}