/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.CountMe;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import java.util.Arrays;
import java.util.function.Function;

/**
 *
 *
 * TODO cleanup the concepts and the API
 *  what is an IndexResultSet?
 *  We shouldn't be having the need for a key;
 *  The concept of a key belongs to the single key result set
 *  an index result set is a range of values in an ordered index
 *  Single key result set is a range of values for a single key (a single rocksdb key
 *  as opposed to the logical hgdb level key ).
 * @param <T> TODO
 *
 */
public class IndexResultSet <T> implements HGRandomAccessResult<T>,
        CountMe
{

    private final RocksDB rocksDB;
    private final LogicalDatabase logicalDB;
    /**
     * The iterato
     */
    RocksIterator iterator;
    /*
    The key whi
     */
    byte[] logicalKey;
    Function<T, byte[]> toByteConverter;
    private final Function<byte[], T> fromByteConverter;

    /**
     *
     * @param rocksDB the database this result set is part of
     * @param iterator the rocksdb iterator which constitutes this result set
     * @param logicalKey the logical key
     * @param toByteConverter used to convert to binary data
     * @param fromByteConverter used to convert from binary data
     */
    public IndexResultSet(
            RocksDB rocksDB,
            LogicalDatabase logicalDB,
            RocksIterator iterator,
            byte[] logicalKey,
            Function<T, byte[]> toByteConverter,
            Function<byte[], T> fromByteConverter)
    {
        this.rocksDB = rocksDB;
        this.logicalDB = logicalDB;
        this.iterator = iterator;
        this.logicalKey = logicalKey;
        this.toByteConverter = toByteConverter;
        this.fromByteConverter = fromByteConverter;
    }

    /*
    Go to a value within this result set
     */
    @Override
    public GotoResult goTo(T value, boolean exactMatch)
    {
        byte[] valueBytes = toByteConverter.apply(value);
        /*
        TODO this should be absracted into the Database class
         */
        byte[] keyvalue = this.logicalDB.scopeKey(this.logicalKey, valueBytes);
        iterator.seek(keyvalue);
        if (iterator.isValid())
        {
            return GotoResult.found;
        }
        else
        {
            return GotoResult.nothing;
        }
    }

    @Override
    public void goAfterLast()
    {
        iterator.seekToLast();
    }

    @Override
    public void goBeforeFirst()
    {
        iterator.seekToFirst();
    }

    @Override
    public T current()
    {
        if (iterator.isValid())
        {
            var keyvalue = iterator.key();
            var value = Arrays.copyOfRange(keyvalue, this.logicalKey.length, keyvalue.length);
            return fromByteConverter.apply(value);
        }
        return null;
    }

    @Override
    public void close()
    {
        this.iterator.close();
    }

    @Override
    public boolean isOrdered()
    {
        return true;
    }

    @Override
    public boolean hasPrev()
    {
        //TODO
        return true;
    }

    @Override
    public T prev()
    {
        this.iterator.prev();
        if (iterator.isValid())
        {
            return this.current();
        }
        return null;
    }

    @Override
    public boolean hasNext()
    {
        //TODO
        return true;
    }

    @Override
    public T next()
    {
        this.iterator.next();
        if (iterator.isValid())
        {
            return this.current();
        }
        return null;
    }

    @Override
    public int count()
    {
        /*
        we need to count the records between
         */
        var start = new Slice(this.logicalDB.firstGlobalKeyForLogicalKey(this.logicalKey));
        var end = new Slice(this.logicalDB.lastGlobalKeyForLogicalKey(this.logicalKey));
        return (int)this.rocksDB.getApproximateMemTableStats(new Range(start, end)).count;
    }
}
