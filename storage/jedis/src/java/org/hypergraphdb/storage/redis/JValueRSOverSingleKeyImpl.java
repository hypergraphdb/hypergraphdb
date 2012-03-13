/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.redis;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * <p>
 * Implements a result set that iterates over all values of a given key.
 * </p>
 */
public class JValueRSOverSingleKeyImpl<ValueType>
{
    private byte[] key;
    private final ByteArrayConverter<ValueType> valueConverter;
    private final JedisStore storage;
    private static Map<byte[], JValueRSOverSingleKeyImpl> jSKRSRegistry = new HashMap<byte[], JValueRSOverSingleKeyImpl>();   // hash value of Set<KeyType>?
    private final int indexID;

    private JValueRSOverSingleKeyImpl(JedisStore storage, int indexID, byte[] key, ByteArrayConverter<ValueType> valueConverter) {
        this.storage = storage;
        this.indexID = indexID;
        this.key = key;
        this.valueConverter = valueConverter;
    }

    public static <ValueType> JValueRSOverSingleKeyImpl getJedisSingleKeyResultSet(byte[] key, JedisStore storage, int jedisdbid, ByteArrayConverter<ValueType> valueConverter, boolean useCache) {
        if (!useCache)
            return new JValueRSOverSingleKeyImpl(storage, jedisdbid, key, valueConverter);

        else // use cache
        {
            if (jSKRSRegistry.containsKey(key))
                return jSKRSRegistry.get(key);
            else {
                JValueRSOverSingleKeyImpl newJSKRS = new JValueRSOverSingleKeyImpl(storage, jedisdbid, key, valueConverter);
                jSKRSRegistry.put(key, newJSKRS);
                return newJSKRS;
            }
        }
    }

    public HGRandomAccessResult.GotoResult goTo(ValueType value, boolean exactMatch, int[] currentPosition) {
//        System.out.println("JSingleKeyResultSetImpl goTo .IndexID: " + indexID + ". key: " + key + ". currentPosition[0]: " +currentPosition[0]);
        Long zrank = storage.zrank(indexID, key, valueConverter.toByteArray(value));
        HGRandomAccessResult.GotoResult result = HGRandomAccessResult.GotoResult.nothing;
        if (!(zrank == null)) {
            currentPosition[0] = zrank.intValue();
            return HGRandomAccessResult.GotoResult.found;
        }
        if (!exactMatch) {
            int insPoint = storage.getRankOrInsertionPoint(indexID, key, valueConverter.toByteArray(value));
            if (storage.zcard(indexID, key) <= insPoint)
                result = HGRandomAccessResult.GotoResult.nothing;
            else {
                currentPosition[0] = insPoint;
                result = HGRandomAccessResult.GotoResult.close;
            }
        }
        return result;
    }

    public ValueType current(int[] currentPosition) {
//        System.out.println("JSingleKeyResultSetImpl current.IndexID: " + indexID + ". key: " + key + ". currentPosition[0]: " +currentPosition[0]);
        ValueType result = null;
        if (currentPosition[0] == -1 || currentPosition[0] == -2)
            throw new NoSuchElementException();
        byte[] temp = storage.zrangeAt(indexID, key, currentPosition[0]);
        if (!(temp==null))
            result = valueConverter.fromByteArray(temp);
 //       System.out.println("JSingleKeyResultSetImpl current. result null?" + (result==null));

        return result;

    }

    public void close() {}

    public boolean isOrdered() {
        return true;
    }    // TODO -- check isOrdered immer true?

    public boolean hasPrev(int[] currentPosition) {
//        System.out.println("JSingleKeyResultSetImpl hasPrev.IndexID: " + indexID + ". key: " + key + ". currentPosition[0]: " +currentPosition[0]);
        if (currentPosition[0] == -1 || currentPosition[0] == 0 || key == null)
            return false;
        else
            return true;
    }

    public ValueType prev(int[] currentPosition) {
//        System.out.println("JSingleKeyResultSetImpl prev.IndexID: " + indexID + ". key: " + key + ". currentPosition[0]: " +currentPosition[0]);
        if (!hasPrev(currentPosition))
            throw new NoSuchElementException();
        else {
            if (currentPosition[0] == -2) currentPosition[0] = storage.zcard(indexID,key).intValue();
            currentPosition[0] = currentPosition[0] - 1;
            return current(currentPosition);
        }
    }

    public boolean hasNext(int[] currentPosition) {
        if (currentPosition[0] == -2 || currentPosition[0] +1 > storage.zcard(indexID, key) - 1 || key == null)
            //since currentPosition[0] is zero-based, zcard not, AND we are talking about the next element, it must be currentPosition[0] >>>+1<<< Bigger Than storage.zcard(indexID, key) - 1
            return false;
        else
            return true;
    }

    public ValueType next(int[] currentPosition) {
//        System.out.println("JSingleKeyResultSetImpl next.IndexID: " + indexID + ". key: " + key + ". currentPosition[0]: " +currentPosition[0]);
        if (!hasNext(currentPosition))
            throw new NoSuchElementException();
        else {
            currentPosition[0] = currentPosition[0] + 1;
            return current(currentPosition);
        }
    }
    public void remove(int[] currentPosition) {}
    public int count() {
        return storage.zcard(indexID, key).intValue();
    }
}
