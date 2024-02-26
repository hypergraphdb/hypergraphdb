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
 * Single valued logical DB.
 * A logical DB which allows for efficient storage of single value for a key.
 * The keys and values are allowed to be of variable size.
 * The keys are stored as RocksDB keys and values are stored directly as
 * RocksDB values.
 * The records are ordered according to the column family's comparator,
 * which is expected to have been set to the user provided key comparator.
 * TODO consider enforcing this in this class -- this class creates the
 * 	column family and configures its comparator
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
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(Transaction tx, byte[] localKey, byte[] value)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void printDB()
	{
		throw new IllegalStateException();
	}

	@Override
	public ValueIterator<byte[]> iterateKeys(Transaction tx)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueIterator<byte[]> iterateValues(Transaction tx)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueIterator<byte[]> iterateLTE(Transaction tx, byte[] byteArray)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueIterator<byte[]> iterateGTE(Transaction tx, byte[] byteArray)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void printDB(Transaction tx)
	{
		throw new UnsupportedOperationException();
	}
}
