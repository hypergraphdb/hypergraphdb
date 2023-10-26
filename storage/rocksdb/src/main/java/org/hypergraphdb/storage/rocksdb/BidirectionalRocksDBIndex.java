/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.rocksdb.*;

public class BidirectionalRocksDBIndex<IndexKey, IndexValue>
        extends RocksDBIndex<IndexKey, IndexValue>
        implements HGBidirectionalIndex<IndexKey, IndexValue>
{

    private final ColumnFamilyHandle inverseCFHandle;

    public BidirectionalRocksDBIndex(String name,
            ColumnFamilyHandle columnFamily,
            ColumnFamilyHandle inverseCFHandle,
            HGTransactionManager transactionManager,
            ByteArrayConverter<IndexKey> keyConverter,
            ByteArrayConverter<IndexValue> valueConverter,
            TransactionDB db)
    {
        super(name, columnFamily, transactionManager, keyConverter,
                valueConverter, db);
        this.inverseCFHandle = inverseCFHandle;
    }

    @Override
    public void addEntry(IndexKey key, IndexValue value)
    {
        byte[] keyBytes = this.keyConverter.toByteArray(key);
        byte[] valueBytes = this.valueConverter.toByteArray(value);
        /*
        the key is the value, the value is the key
         */
        byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(valueBytes, keyBytes);
        try
        {
            txn().rocksdbTxn().put(inverseCFHandle, rocksDBKey, new byte[0]);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public HGRandomAccessResult<IndexKey> findByValue(IndexValue value)
    {
        return new IteratorResultSet<IndexKey>(
                txn().rocksdbTxn().getIterator(new ReadOptions()
                                .setIterateLowerBound(new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(valueConverter.toByteArray(value))))
                                .setIterateUpperBound(new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(valueConverter.toByteArray(value)))),
                        inverseCFHandle), false)
        {
            @Override
            protected IndexKey extractValue()
            {
                var valueBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(this.iterator.key());
                return keyConverter.fromByteArray(valueBytes, 0, valueBytes.length);
            }

            @Override
            protected byte[] toRocksDBKey(IndexKey key)
            {
                /*
                Intentionally reversed, the values in the result set are
                values in the column family, but keys from the original
                index pov
                 */
                return VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(
                        valueConverter.toByteArray(value),
                        keyConverter.toByteArray(key));
            }
        };
    }

    @Override
    public IndexKey findFirstByValue(IndexValue value)
    {
        byte[] valueBytes = this.valueConverter.toByteArray(value);
        byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(valueBytes);
        try
        {
            byte[] bytes = txn().rocksdbTxn().get(inverseCFHandle, new ReadOptions(), rocksDBKey);
            return keyConverter.fromByteArray(bytes, 0, bytes.length);
        }
        catch (RocksDBException e)
        {
            throw new HGException(e);
        }
    }

    @Override
    public long countKeys(IndexValue value)
    {
        try (var rs = (IteratorResultSet<IndexKey>)findByValue(value) )
        {
            return rs.count();
        }
    }
}


