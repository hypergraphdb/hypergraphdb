package org.hypergraphdb.storage.redis;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.CountMe;

import java.util.Set;

// This class was parametrized <KeyType, ValueType> before. Check old backups in case this needs to be reverted!

public final class JValueRSOverMultiKeys<ValueType> extends JIndexResultSet<ValueType> implements CountMe
{
    private final ByteArrayConverter<ValueType> valueConverter;
    private final JValueRSOverMultiKeysImpl<ValueType> resultSet;
    private JedisStore storage;
    protected int[] currentPosition = {-1, -1};

    public JValueRSOverMultiKeys(JedisStore stor, int jedisdbid, Set<byte[]> keySet, ByteArrayConverter<ValueType> valueConverter, byte[] keycontext, boolean downwards, boolean useCache) {
        this.valueConverter = valueConverter;
        this.storage = stor;
        this.resultSet = JValueRSOverMultiKeysImpl.getJedisMultiKeyValueResultSet(stor, jedisdbid, keySet, keycontext, valueConverter, downwards, useCache);
    }

    public HGRandomAccessResult.GotoResult goTo(ValueType value, boolean exactMatch) {
        int[] in = resultSet.goTo(value,exactMatch);
        if(in[2]==1)
            {
                currentPosition[0] = in[0];
                currentPosition[1] = in[1];
                return HGRandomAccessResult.GotoResult.close;
            }
        if(in[2]==0)
            {
                currentPosition[0] = in[0];
                currentPosition[1] = in[1];
                return HGRandomAccessResult.GotoResult.found;
            }
         else
            return HGRandomAccessResult.GotoResult.nothing;
    }


    public void goAfterLast() {
        currentPosition[0] = -2;
        currentPosition[1] = -2;
    }

    public void goBeforeFirst() {
        currentPosition[0] = -1;
        currentPosition[1] = -1;    }


    public ValueType current() {
        return resultSet.current(currentPosition);
    }

    public void close() {
        
    }

    public boolean isOrdered() {
        return false;  
    }

    public boolean hasPrev() {
        return resultSet.hasPrev(currentPosition);
    }

    public ValueType prev() {
        return resultSet.prev(currentPosition);
    }

    public boolean hasNext() {
        return resultSet.hasNext(currentPosition);
    }

    public ValueType next() {
        return resultSet.next(currentPosition);
    }

    public void remove() {
        
    }

    public int count() { return resultSet.count();  }
    public void removeCurrent() {   throw new HGException ("Not implemented in JedisStorImpl"); }
}
