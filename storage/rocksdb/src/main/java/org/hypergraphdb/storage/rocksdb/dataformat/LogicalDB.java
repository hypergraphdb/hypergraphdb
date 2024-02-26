/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;

import org.hypergraphdb.storage.rocksdb.iterate.ValueIterator;
import org.rocksdb.*;

/**
 *
 * A LogicalDB is a key-value store for related keys (as in identifiers
 * of hgdb objects, not RocksDB keys).
 * <br/>
 * <br/>
 * A LogicalDB is backed by a RocksDB column family whose records have a specific record format.
 * <br/>
 * <br/>
 * Different LogicalDB subclasses can employ different record formats in order to support
 * different scenarios efficiently: e.g.
 * - keys with fixed size / variable size,
 * - values with fixed size / variable size,
 * - single value for a key / multiple values for a key
 * <br/>
 * <br/>
 * In other words different LogicalDB subclasses fit the different storage
 * needs for a given space of records.
 * <br/>
 * <br/>
 * The LogicalDB supports CRUD operations for key - value pairs.
 * Some LogicalDB subclasses might not support some operations if their record format
 * does not support them.
 * <br/>
 * <br/>
 * The key spaces for the LogicalDBs are independent i.e. different LogicalDBs
 * can hold different records under the same key.
 * <br/>
 * <br/>
 * Note on column families (CF) -- a column family is a set of related records in
 * RocksDB. Each CF is an independent key space i.e. different CFs can have records
 * for the same key. CFs are roughly equivalent to BerkeleyDB databases.
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

	/**
	 * @return the name of the column family backing this LogicalDB
	 */
	public String cfName()
	{
		return this.columnFamilyName;
	}

	@Override
	public void close()
	{
		this.cfOptions.close();
	}

	/**
	 * Put a record in this LogicalDB
	 * @param tx the transaction to use to perform the operation
	 */
	public abstract void put(Transaction tx, byte[] key, byte[] value);

	/**
	 * Retrieve a record
	 * @return if the LogicalDB contains values for the record, return
	 * the first one. Else, return null
	 */
	public abstract byte[] get(Transaction tx, byte[] key);

	/**
	 * Delete all values associated with a key in the LogicalDB
	 */
	public abstract void delete(Transaction tx, byte[] key);

	/**
	 * Iterate all the values for a given key
	 */
	public abstract ValueIterator<byte[]> iterateValuesForKey(Transaction tx, byte[] byteArray);

	/**
	 * Delete a specific key - value combination
	 */
	public abstract void delete(Transaction tx, byte[] localKey, byte[] value);

	/**
	 * Print the contents of the db to the console for debugging purposes
	 */
	public abstract void printDB();

	/**
	 * Iterate all the keys in the LogicalDB
	 * @param tx the transaction to use to perform the operation
	 * @return a ValueIterator containing the values stored under a given key in this
	 * LogicalDB
	 */
	public abstract ValueIterator<byte[]> iterateKeys(Transaction tx);

	/**
	 * Iterate all the values in the LogicalDB
	 */
	public abstract ValueIterator<byte[]> iterateValues(Transaction tx);

	/**
	 * Iterate all the values for every key which is less than a given key
	 */
	public abstract ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray);

	/**
	 * Iterate all the values for every key which is less than or equal to a given key
	 */
	public abstract ValueIterator<byte[]> iterateLTE(Transaction tx,
			byte[] byteArray);

	/**
	 * Iterate all the values for every key which is greater than given key
	 */
	public abstract ValueIterator<byte[]> iterateGT(Transaction tx,
			byte[] byteArray);

	/**
	 * Iterate all the values for every key which is greater than or equal to a given key
	 */
	public abstract ValueIterator<byte[]> iterateGTE(Transaction tx,
			byte[] byteArray);

	/**
	 * print a db for debugging purposes including changes made in a given transaction
	 */
	public abstract void printDB(Transaction tx);

	public ColumnFamilyHandle cfHandle()
	{
		return this.columnFamilyHandle;
	}

}
