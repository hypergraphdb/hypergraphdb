/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.rocksdb.iterate.ValueIterator;
import org.rocksdb.*;

/**
 * Single valued DB
 */
public class SVDB extends LogicalDB
{

	public SVDB(String name, ColumnFamilyHandle cfHandle,
			ColumnFamilyOptions cfOptions)
	{
		super(name, cfHandle, cfOptions);
	}

	@Override
	public void put(Transaction tx, byte[] key, byte[] value)
	{
		try
		{
			tx.put(this.columnFamilyHandle, key, value);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public byte[] get(Transaction tx, byte[] key)
	{
		try (var ro = new ReadOptions().setSnapshot(tx.getSnapshot()))
		{
			return tx.get(this.columnFamilyHandle, ro, key);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public void delete(Transaction tx, byte[] key)
	{
		try
		{
			tx.delete(this.columnFamilyHandle, key);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public ValueIterator<byte[]> iterateValuesForKey(Transaction tx,
			byte[] byteArray)
	{
		throw new IllegalStateException();
	}

	@Override
	public void delete(Transaction tx, byte[] localKey, byte[] value)
	{
		throw new IllegalStateException();
	}

	@Override
	public void printDB()
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateKeys(Transaction tx)
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateValues(Transaction tx)
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray)
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateLTE(Transaction tx, byte[] byteArray)
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateGTE(Transaction tx, byte[] byteArray)
	{
		throw new IllegalStateException();
	}

	@Override
	public void printDB(Transaction tx)
	{
		throw new IllegalStateException();
	}
}
