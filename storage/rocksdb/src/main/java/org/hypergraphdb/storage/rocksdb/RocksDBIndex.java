/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGIndexStats;
import org.rocksdb.ColumnFamilyHandle;

import java.util.Comparator;

/**
 * An index stored in the
 * @param <Key>
 * @param <Value>
 */
public class RocksDBIndex<Key, Value> implements HGIndex<Key, Value>
{
    private final ByteArrayConverter<Key> keyConverter;
    private final ByteArrayConverter<Value> valueConverter;
    private final Comparator<byte[]> keyComparator;
    private final Comparator<byte[]> valueComparator;


    private boolean isInitialized = false;

    public RocksDBIndex(
        String name,
        ByteArrayConverter<Key> keyConverter,
        ByteArrayConverter<Value> valueConverter,
        Comparator<byte[]> keyComparator,
        Comparator<byte[]> valueComparator,
        boolean isBidirectional)
    {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.keyComparator = keyComparator;
        this.valueComparator = valueComparator;
        /*
        we have multiple values for each key
        we must combine the keys and value
        we need to create an

        1. key: byte array with arbitrary length
        2. value: byte array with arbitrary length
        we know how to compare key to key and value to value

        if we know where is the key and weher

        byte[0] = N
        byte[1 - N+1] - key
        byte[N+2 - ] value

         */


    }
    @Override
    public String getName()
    {
        return null;
    }

    private ColumnFamilyHandle indexCF()
    {
        //the comparator is set when we create the column family
        return null;
    }

    private StorageTransactionRocksDB txn()
    {
        return null;
    }

    @Override
    public void addEntry(Key key, Value value)
    {

//        txn().rocksdbTxn().put();


    }

    @Override
    public void removeEntry(Key key, Value value)
    {

    }

    @Override
    public void removeAllEntries(Key key)
    {

    }

    @Override
    public Value findFirst(Key key)
    {
        return null;
    }

    @Override
    public HGRandomAccessResult<Value> find(Key key)
    {
        return null;
    }

    @Override
    public void open()
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean isOpen()
    {
        return false;
    }

    @Override
    public HGRandomAccessResult<Key> scanKeys()
    {
        return null;
    }

    @Override
    public HGRandomAccessResult<Value> scanValues()
    {
        return null;
    }

    @Override
    public long count()
    {
        return 0;
    }

    @Override
    public long count(Key key)
    {
        return 0;
    }

    @Override
    public HGIndexStats<Key, Value> stats()
    {
        return null;
    }
}
