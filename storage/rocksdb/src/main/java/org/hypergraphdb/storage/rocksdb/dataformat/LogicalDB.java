/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.rocksdb.iterate.DistinctValueIterator;
import org.hypergraphdb.storage.rocksdb.iterate.ValueIterator;
import org.rocksdb.*;

import java.util.Arrays;
import java.util.List;


/**
 *
 * A logical database which is backed by a column family in RocksDB
 * Different implementations have different serialization mechanisms which
 * correspond to the storage needs
 *
 */
public abstract class LogicalDB implements AutoCloseable
{
	protected final String columnFamilyName;
	protected final ColumnFamilyHandle columnFamilyHandle;
	private final ColumnFamilyOptions cfOptions;

	public LogicalDB(String name, ColumnFamilyHandle columnFamilyHandle, ColumnFamilyOptions columnFamilyOptions)
	{
		this.columnFamilyHandle = columnFamilyHandle;
		this.cfOptions = columnFamilyOptions;
		this.columnFamilyName = name;
	}
	public String cfName()
	{
		return this.columnFamilyName;
	}

	@Override
	public void close()
	{
		this.cfOptions.close();
	}

	public abstract void put(Transaction tx, byte[] key, byte[] value);

	public abstract byte[] get(Transaction tx, byte[] key);

	public abstract void delete(Transaction tx, byte[] key);

	/**
	 * Get the iterator result set
	 */
	public abstract ValueIterator<byte[]> iterateValuesForKey(Transaction tx, byte[] byteArray);

	public abstract void delete(Transaction tx, byte[] localKey, byte[] value);

	public abstract void printDB();

	public abstract ValueIterator<byte[]> iterateKeys(Transaction tx);

	public abstract ValueIterator<byte[]> iterateValues(Transaction tx);

	public abstract ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray);

	public abstract ValueIterator<byte[]> iterateLTE(Transaction tx,
			byte[] byteArray);

	public abstract ValueIterator<byte[]> iterateGT(Transaction tx,
			byte[] byteArray);

	public abstract ValueIterator<byte[]> iterateGTE(Transaction tx,
			byte[] byteArray);

	public abstract void printDB(Transaction tx);

	public ColumnFamilyHandle cfHandle()
	{
		return this.columnFamilyHandle;
	}

}
