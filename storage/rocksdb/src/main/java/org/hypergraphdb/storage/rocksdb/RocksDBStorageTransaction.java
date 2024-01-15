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
import org.hypergraphdb.transaction.TransactionConflictException;
import org.rocksdb.*;

/**
 * An adapter for a RocksDB transaction
 */
public class RocksDBStorageTransaction implements HGStorageTransaction
{
	private final Transaction txn;
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
			WriteOptions writeOptions)
	{
		this.txn = txn;
		this.writeOptions = writeOptions;
	}

	/**
	 *
	 */
	private void close()
	{
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
			Status s = e.getStatus();
			//TODO do we need to throw transaction conflict only when the status is Busy
//            s.getCode().equals(Status.Code.Busy);
			throw new TransactionConflictException();
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
