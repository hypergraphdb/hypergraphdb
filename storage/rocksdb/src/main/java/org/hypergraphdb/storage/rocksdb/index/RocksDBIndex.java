/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.index;

import org.hypergraphdb.*;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGIndexStats;
import org.hypergraphdb.storage.rocksdb.IteratorResultSet;
import org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB;
import org.hypergraphdb.storage.rocksdb.dataformat.VarKeyVarValueColumnFamilyMultivaluedDB;
import org.rocksdb.*;

import java.util.List;

/**
 * An index stored in the
 * @param <IndexKey>
 * @param <IndexValue>
 */
public class RocksDBIndex<IndexKey, IndexValue> implements HGSortIndex<IndexKey, IndexValue>
{
    private final ColumnFamilyHandle columnFamily;
    private final String name;
    protected final ByteArrayConverter<IndexKey> keyConverter;
    protected final ByteArrayConverter<IndexValue> valueConverter;
    private final OptimisticTransactionDB db;
    private volatile boolean open = true;
    public final StorageImplementationRocksDB store;

    public RocksDBIndex(
            String name,
            ColumnFamilyHandle columnFamily,
            ByteArrayConverter<IndexKey> keyConverter,
            ByteArrayConverter<IndexValue> valueConverter,
            OptimisticTransactionDB db,
            StorageImplementationRocksDB store)

    {
        this.name = name;
        this.columnFamily = columnFamily;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.db = db;
        this.store = store;
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
    public void open()
    {
        /*
        This implementation does not need to be explicitly opened
        because all the necessary resources are opened when RocksDB
        is initialized or when the index is created.
        The HGDB contract expects the is open state to work as expected
        though
         */
        /*
        TODO consider reopening any possibly closed objects here
         */
        this.open = true;
    }

    @Override
    public void close()
    {
       /*
       Resources logically held by the index are
       Column family
       Column family options
       However their lifecycle has to be managed by the storage layer
       ...
        */
        this.open = false;
    }

    public void checkOpen() throws HGException
    {
        if (!this.isOpen())
        {
            throw new HGException("The index is not open.");
        }
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }


    @Override
    public String getName()
    {
        return this.name;
    }



    @Override
    public void addEntry(IndexKey key, IndexValue value)
    {
        checkOpen();
        byte[] keyBytes = this.keyConverter.toByteArray(key);
        byte[] valueBytes = this.valueConverter.toByteArray(value);
        byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(keyBytes, valueBytes);
        this.store.ensureTransaction(tx -> {
            try
            {
                tx.put(columnFamily, rocksDBKey, new byte[0]);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }

    @Override
    public void removeEntry(IndexKey key, IndexValue value)
    {
        checkOpen();
        byte[] keyBytes = this.keyConverter.toByteArray(key);
        byte[] valueBytes = this.valueConverter.toByteArray(value);
        byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(keyBytes, valueBytes);
        this.store.ensureTransaction(tx -> {
            try
            {
                tx.delete(columnFamily, rocksDBKey);
            }
            catch (RocksDBException e)
            {
                throw new HGException(e);
            }
        });
    }

    @Override
    public void removeAllEntries(IndexKey key)
    {
        checkOpen();
        /*
            TODO consider using RangeDelete which is not yet supported by transactions
         */


//        try
//        {
//            db.deleteRange(
//                    columnFamily,
//                    VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyConverter.toByteArray(key)),
//                    VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyConverter.toByteArray(key)));
//        }
//        catch (RocksDBException e)
//        {
//            throw new HGException(e);
//        }
        this.store.ensureTransaction(tx -> {
            try (
                    var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyConverter.toByteArray(key)));
                    var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyConverter.toByteArray(key)));
                    var ro = new ReadOptions()
                            .setSnapshot(tx.getSnapshot())
                            .setIterateLowerBound(lower)
                            .setIterateUpperBound(upper);
                    RocksIterator iterator  = tx.getIterator(ro, columnFamily))
            {
                iterator.seekToFirst();
                while (iterator.isValid())
                {
                    byte[] next = iterator.key();
                    try
                    {
                        tx.delete(columnFamily, next);
                    }
                    catch (RocksDBException e)
                    {
                        throw new HGException(e);
                    }
                    iterator.next();
                }
                try
                {
                    iterator.status();
                }
                catch (RocksDBException e)
                {
                    throw new HGException(e);
                }
            }

        });

    }

    @Override
    public IndexValue findFirst(IndexKey key)
    {
        checkOpen();
        byte[] keyBytes = this.keyConverter.toByteArray(key);
        byte[] firstRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyBytes);
        byte[] lastRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyBytes);
        return this.store.ensureTransaction(tx -> {
            try (var lower = new Slice(firstRocksDBKey);
                 var upper = new Slice(lastRocksDBKey);
                 var ro = new ReadOptions()
                         .setSnapshot(tx.getSnapshot())
                         .setIterateLowerBound(lower)
                         .setIterateUpperBound(upper);
                RocksIterator iterator = tx.getIterator(ro, columnFamily);
            )
            {
                iterator.seekToFirst();

                if (iterator.isValid())
                {
                    byte[] bytes = iterator.key();
                    var valuebytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(bytes);
                    return valueConverter.fromByteArray(valuebytes, 0, valuebytes.length);
                }
                else
                {
                    try
                    {
                        iterator.status();
                    }
                    catch (RocksDBException e)
                    {
                        throw new HGException(e);
                    }
                    return null;
                }

            }

        });

    }

    @Override
    public HGRandomAccessResult<IndexValue> find(IndexKey key)
    {
        checkOpen();
        return this.store.ensureTransaction(tx -> {
            var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyConverter.toByteArray(key)));
            var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyConverter.toByteArray(key)));
            var ro = new ReadOptions()
                    .setSnapshot(tx.getSnapshot())
                    .setIterateLowerBound(lower)
                    .setIterateUpperBound(upper);
            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily), List.of(lower, upper, ro), false)
            {
                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                    return VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(
                            keyConverter.toByteArray(key),
                            valueConverter.toByteArray(value));

                }
            };

        });
    }


    @Override
    public HGRandomAccessResult<IndexKey> scanKeys()
    {
        checkOpen();
        /*
        TODO the 'values' in the result set are the 'keys' in the index
            which is confusing

        TODO in the lmdb implementation, the result set returns the unique
            keys. Here we are returning all the keys in the iterator
         */
        return this.store.ensureTransaction(tx -> {
            var ro = new ReadOptions()
                    .setSnapshot(tx.getSnapshot());
            return new IteratorResultSet<IndexKey>(tx.getIterator(ro, columnFamily), List.of(ro), true)
            {
                @Override
                protected IndexKey extractValue()
                {
                    var keyBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractKey(this.iterator.key());
                    return keyConverter.fromByteArray(keyBytes, 0, keyBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexKey value)
                {
                /*
                The first rocksdb key with the given logical key (which is
                actually a value in the result set)

                There could be more than one value, we return the first
                 */
                    return VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
                            keyConverter.toByteArray(value));
                }
            };

        });

    }

    @Override
    public HGRandomAccessResult<IndexValue> scanValues()
    {
        checkOpen();
        /*
        TODO the 'values' in the result set are the 'keys' in the index
            which is confusing

        in lmdb scan values gives all (even duplicate values)
         */
        return this.store.ensureTransaction(tx -> {
            var ro = new ReadOptions().setSnapshot(
                    tx.getSnapshot());
            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily),
                    List.of(ro),
                    false)
            {
                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                /*
                We have only the value. we cannot possibly recreate the
                full rocks db key using only that. we need the logical key
                as well
                 */
                    throw new UnsupportedOperationException("Cannot create a rocks db" +
                            "key given only the index value");
                }
            };

        });
    }

    @Override
    public long count()
    {
        checkOpen();
        try (var rs = (IteratorResultSet<IndexKey>)scanKeys())
        {
            return rs.count();
        }
    }

    @Override
    public long count(IndexKey key)
    {
        checkOpen();
        try (var rs = (IteratorResultSet<IndexValue>)find(key) )
        {
            return rs.count();
        }
    }

    /**
     * estimates the number of records in a given range
     * @param startKey
     * @param endKey
     * @return
     */
    long estimateIndexRange(byte[] startKey, byte[] endKey)
    {
        checkOpen();
        try (Slice start = new Slice(startKey); Slice end = new Slice(endKey))
        {
            var range = new Range(start, end);


            var memtableStats = db.getApproximateMemTableStats(columnFamily,range);
            long avgRecordSize = 0;
            if (memtableStats.count == 0)
            {
               if (memtableStats.size != 0)
                   avgRecordSize = Integer.MAX_VALUE;
            }
            else
            {
                avgRecordSize = memtableStats.size / memtableStats.count;
            }

            var sizeOnDisk = db.getApproximateSizes(columnFamily, List.of(range))[0];
            if (avgRecordSize == 0)
            {
               if(sizeOnDisk == 0)
                   return memtableStats.count;
               else
                   return Integer.MAX_VALUE;
            }
            else
            {
                return memtableStats.count + sizeOnDisk/avgRecordSize;
            }
            /*
            the size on disk is the compressed size. Ideally we would have
            an estimation of the compression factor.
             */


        }

    }

    long estimateIndexSize()
    {
        checkOpen();
        return estimateIndexRange(
                VarKeyVarValueColumnFamilyMultivaluedDB.globallyFirstRocksDBKey(),
                VarKeyVarValueColumnFamilyMultivaluedDB.globallyLastRocksDBKey());
    }

    @Override
    public HGIndexStats<IndexKey, IndexValue> stats()
    {
        checkOpen();
        return new RocksDBIndexStats<IndexKey, IndexValue>(this);
    }

    @Override
    public HGSearchResult<IndexValue> findLT(IndexKey key)
    {
        checkOpen();
        return this.store.ensureTransaction(tx -> {
            var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyConverter.toByteArray(key)));
            var ro = new ReadOptions()
                    .setIterateUpperBound(upper)
                    .setSnapshot(tx.getSnapshot());
            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily), List.of(upper, ro), false)
            {

                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                    throw new UnsupportedOperationException("Connot convert value to a complete key.");
                }
            };

        });
    }

    @Override
    public HGSearchResult<IndexValue> findGT(IndexKey key)
    {
        checkOpen();
        return this.store.ensureTransaction(tx -> {
            var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyConverter.toByteArray(key)));
            var ro = new ReadOptions()
                    .setIterateLowerBound(lower)
                    .setSnapshot(tx.getSnapshot());
            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily), List.of(lower, ro), false)
            {

                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                    throw new UnsupportedOperationException("Connot convert value to a complete key.");
                }
            };

        });
    }

    @Override
    public HGSearchResult<IndexValue> findLTE(IndexKey key)
    {
        checkOpen();
        return this.store.ensureTransaction(tx -> {
            var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB
                    .lastRocksDBKey(keyConverter.toByteArray(key)));
            var ro = new ReadOptions()
                    .setSnapshot(tx.getSnapshot())
                    .setIterateUpperBound(upper);
            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily), List.of(upper, ro), false)
            {

                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                    throw new UnsupportedOperationException("Connot convert value to a complete key.");
                }
            };

        });
    }

    @Override
    public HGSearchResult<IndexValue> findGTE(IndexKey key)
    {
        checkOpen();
        return this.store.ensureTransaction(tx -> {
            var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB
                    .firstRocksDBKey(keyConverter.toByteArray(key)));
            var ro = new ReadOptions()
                    .setSnapshot(tx.getSnapshot())
                    .setIterateLowerBound(lower);

            return new IteratorResultSet<IndexValue>(
                    tx.getIterator(ro, columnFamily), List.of(lower, ro),false)
            {

                @Override
                protected IndexValue extractValue()
                {
                    var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                    return valueConverter.fromByteArray(valueBytes, 0, valueBytes.length);
                }

                @Override
                protected byte[] toRocksDBKey(IndexValue value)
                {
                    throw new UnsupportedOperationException("Connot convert value to a complete key.");

                }
            };

        });
    }
}
