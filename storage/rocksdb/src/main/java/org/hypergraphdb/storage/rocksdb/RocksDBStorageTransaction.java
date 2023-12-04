/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

/**
 * An adapter for a RocksDB transaction
 */
public class RocksDBStorageTransaction implements HGStorageTransaction
{
    private final Transaction txn;
//    private final TransactionOptions txnOptions;
    private final WriteOptions writeOptions;

    public static RocksDBStorageTransaction nullTransaction()
    {
        return new RocksDBStorageTransaction(null, null);
    }

    public Transaction rocksdbTxn()
    {
        return txn;
    }

    public RocksDBStorageTransaction(
            Transaction txn,
//            TransactionOptions txnOptions,
            WriteOptions writeOptions)
    {
        this.txn = txn;
//        this.txnOptions = txnOptions;
        this.writeOptions = writeOptions;
    }

    /**
     *
     */
    private void close()
    {
//        this.txnOptions.close();
        this.writeOptions.close();
    }

    @Override
    public void commit() throws HGTransactionException
    {
        try
        {
            if (txn != null)
                this.txn.commit();
        }
        catch (RocksDBException e)
        {
            throw new HGTransactionException(e);
        }
        finally
        {
            this.close();
        }
    }

    @Override
    public void abort() throws HGTransactionException
    {
        try
        {
            if (txn != null)
                this.txn.rollback();
        }
        catch (RocksDBException e)
        {
            throw new HGTransactionException(e);
        }
        finally
        {
            this.close();
        }

    }
}
