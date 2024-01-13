/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.index;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.rocksdb.dataformat.LogicalDB;
import org.hypergraphdb.storage.rocksdb.resultset.IteratorResultSet;
import org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB;
import org.hypergraphdb.storage.rocksdb.dataformat.VarKeyVarValueColumnFamilyMultivaluedDB;
import org.rocksdb.*;

import java.util.List;

public class BidirectionalRocksDBIndex<IndexKey, IndexValue>
		extends RocksDBIndex<IndexKey, IndexValue>
		implements HGBidirectionalIndex<IndexKey, IndexValue>
{

	private final ColumnFamilyHandle inverseCFHandle;
	private final String inverseCFName;
	private final ColumnFamilyOptions inverseColumnFamilyOptions;
	private final LogicalDB inverseIndexDB;

	/**
	 *
	 * @param name
	 * @param columnFamily
	 * @param columnFamilyName
	 * @param columnFamilyOptions
	 * @param inverseCFHandle
	 * @param inverseColumnFamilyName
	 * @param inverseColumnFamilyOptions
	 * @param keyConverter the converter for the keys as stored in the forward index
	 * @param valueConverter the converter for the values as stored in the forward index
	 * @param db
	 * @param store
	 */
	public BidirectionalRocksDBIndex(String name,
			ColumnFamilyHandle columnFamily,
			String columnFamilyName,
			ColumnFamilyOptions columnFamilyOptions,
			ColumnFamilyHandle inverseCFHandle,
			String inverseColumnFamilyName,
			ColumnFamilyOptions inverseColumnFamilyOptions,
			ByteArrayConverter<IndexKey> keyConverter,
			ByteArrayConverter<IndexValue> valueConverter,
			OptimisticTransactionDB db,
			StorageImplementationRocksDB store)
	{
		super(
				name,
				columnFamily,
				columnFamilyName,
				columnFamilyOptions,
				keyConverter,
				valueConverter,
				db,
				store);
		this.inverseCFHandle = inverseCFHandle;
		this.inverseCFName = inverseColumnFamilyName;
		this.inverseColumnFamilyOptions = inverseColumnFamilyOptions;
		this.inverseIndexDB = new LogicalDB.VKVVMVDB(inverseColumnFamilyName, inverseCFHandle, inverseColumnFamilyOptions);
	}

	public String getInverseCFName()
	{
		return this.inverseCFName;
	}

	public ColumnFamilyHandle getInverseCFHandle()
	{
		return this.inverseCFHandle;
	}

	@Override
	public void addEntry(IndexKey key, IndexValue value)
	{
		checkOpen();
		super.addEntry(key, value);
		byte[] keyBytes = this.keyConverter.toByteArray(key);
		byte[] valueBytes = this.valueConverter.toByteArray(value);
		/*
		the key is the value, the value is the key
		 */
		this.store.ensureTransaction(tx -> {
				this.inverseIndexDB.put(tx, valueBytes, keyBytes);
		});
	}

	@Override
	public HGRandomAccessResult<IndexKey> findByValue(IndexValue value)
	{
		return this.store.ensureTransaction(tx -> {
			return new IteratorResultSet<>(
					this.inverseIndexDB.iterateValuesForKey(tx, valueConverter.toByteArray(value))
							.map(bytes -> this.keyConverter.fromByteArray(bytes, 0, bytes.length),
									this.keyConverter::toByteArray));
		});
	}

	@Override
	public IndexKey findFirstByValue(IndexValue value)
	{
		checkOpen();
		byte[] valueBytes = this.valueConverter.toByteArray(value);
		return this.store.ensureTransaction(tx -> {
			var keyBytes = this.inverseIndexDB.get(tx, valueBytes);
			return keyBytes == null ? null : keyConverter.fromByteArray(keyBytes, 0, keyBytes.length);
		});
	}

	@Override
	public long countKeys(IndexValue value)
	{
		checkOpen();
		try (var rs = (IteratorResultSet<IndexKey>)findByValue(value) )
		{
			return rs.count();
		}
	}

	@Override
	public void close()
	{
		super.close();
		this.inverseColumnFamilyOptions.close();
	}
}


