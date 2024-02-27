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
import org.hypergraphdb.storage.rocksdb.dataformat.LogicalDB;
import org.hypergraphdb.storage.rocksdb.dataformat.VKVVMVDB;
import org.hypergraphdb.storage.rocksdb.resultset.IteratorResultSet;
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
	private final LogicalDB indexDB;
	private final String name;
	protected final ByteArrayConverter<IndexKey> keyConverter;
	protected final ByteArrayConverter<IndexValue> valueConverter;
	private final OptimisticTransactionDB db;
	private volatile boolean open = true;
	public final StorageImplementationRocksDB store;

	public RocksDBIndex(
			String name,
			ColumnFamilyHandle columnFamily,
			String columnFamilyName,
			ColumnFamilyOptions columnFamilyOptions,
			ByteArrayConverter<IndexKey> keyConverter,
			ByteArrayConverter<IndexValue> valueConverter,
			OptimisticTransactionDB db,
			StorageImplementationRocksDB store)

	{
		this.name = name;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		this.db = db;
		this.store = store;
		this.indexDB = new VKVVMVDB(columnFamilyName, columnFamily, columnFamilyOptions);
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



	public String getColumnFamilyName()
	{
		return this.indexDB.cfName();
	}

	public ColumnFamilyHandle getColumnFamilyHandle()
	{
		return this.indexDB.cfHandle();
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
		this.open = false;
		this.indexDB.close();
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
		this.store.ensureTransaction(tx -> {
			this.indexDB.put(tx, keyBytes, valueBytes);
		});
	}

	@Override
	public void removeEntry(IndexKey key, IndexValue value)
	{
		checkOpen();
		byte[] keyBytes = this.keyConverter.toByteArray(key);
		byte[] valueBytes = this.valueConverter.toByteArray(value);
		this.store.ensureTransaction(tx -> {
			this.indexDB.delete(tx, keyBytes, valueBytes);
		});
	}

	@Override
	public void removeAllEntries(IndexKey key)
	{
		checkOpen();
		this.store.ensureTransaction(tx -> {
			this.indexDB.delete(tx, keyConverter.toByteArray(key));

		});

	}

	@Override
	public IndexValue findFirst(IndexKey key)
	{
		checkOpen();
		byte[] keyBytes = this.keyConverter.toByteArray(key);
		return this.store.ensureTransaction(tx -> {
			var valuebytes = this.indexDB.get(tx, keyBytes);
			return valuebytes == null ? null : valueConverter.fromByteArray(valuebytes, 0, valuebytes.length);
		});

	}

	@Override
	public HGRandomAccessResult<IndexValue> find(IndexKey key)
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			var iterator = this.indexDB.iterateValuesForKey(tx, keyConverter.toByteArray(key))
					.map(bytes -> {
						var value = this.valueConverter.fromByteArray(bytes, 0, bytes.length);
						return value;
					}, this.valueConverter::toByteArray);

			return new IteratorResultSet<>(iterator);
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
			return new IteratorResultSet<>(this.indexDB.iterateKeys(tx).map(
					bytes -> keyConverter.fromByteArray(bytes, 0, bytes.length),
					keyConverter::toByteArray));
		});

	}

	@Override
	public HGRandomAccessResult<IndexValue> scanValues()
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			return new IteratorResultSet<>(
					this.indexDB.iterateValues(tx)
							.map(bytes -> this.valueConverter.fromByteArray(bytes, 0, bytes.length),
									this.valueConverter::toByteArray));
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


			var memtableStats = db.getApproximateMemTableStats(this.indexDB.cfHandle(), range);
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

			var sizeOnDisk = db.getApproximateSizes(this.indexDB.cfHandle(), List.of(range))[0];
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
			return new IteratorResultSet<>(
					this.indexDB.iterateLT(tx,keyConverter.toByteArray(key))
							.map(bytes -> this.valueConverter.fromByteArray(bytes, 0, bytes.length),
									this.valueConverter::toByteArray));
		});
	}

	@Override
	public HGSearchResult<IndexValue> findGT(IndexKey key)
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			return new IteratorResultSet<>(
					this.indexDB.iterateGT(tx,keyConverter.toByteArray(key))
							.map(bytes -> this.valueConverter.fromByteArray(bytes, 0, bytes.length),
									this.valueConverter::toByteArray));
		});
	}

	@Override
	public HGSearchResult<IndexValue> findLTE(IndexKey key)
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			return new IteratorResultSet<>(
					this.indexDB.iterateLTE(tx,keyConverter.toByteArray(key))
							.map(bytes -> this.valueConverter.fromByteArray(bytes, 0, bytes.length),
									this.valueConverter::toByteArray));
		});

	}

	@Override
	public HGSearchResult<IndexValue> findGTE(IndexKey key)
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			return new IteratorResultSet<>(
					this.indexDB.iterateGTE(tx,keyConverter.toByteArray(key))
							.map(bytes -> this.valueConverter.fromByteArray(bytes, 0, bytes.length),
									this.valueConverter::toByteArray));
		});

	}
}
