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
import org.hypergraphdb.storage.rocksdb.IteratorResultSet;
import org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB;
import org.hypergraphdb.storage.rocksdb.dataformat.VarKeyVarValueColumnFamilyMultivaluedDB;
import org.hypergraphdb.storage.rocksdb.index.RocksDBIndex;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.rocksdb.*;

import java.util.List;

public class BidirectionalRocksDBIndex<IndexKey, IndexValue>
		extends RocksDBIndex<IndexKey, IndexValue>
		implements HGBidirectionalIndex<IndexKey, IndexValue>
{

	private final ColumnFamilyHandle inverseCFHandle;
	private final String inverseCFName;

	public BidirectionalRocksDBIndex(String name,
			ColumnFamilyHandle columnFamily,
			String columnFamilyName,
			ColumnFamilyHandle inverseCFHandle,
			String inverseColumnFamilyName,
			ByteArrayConverter<IndexKey> keyConverter,
			ByteArrayConverter<IndexValue> valueConverter,
			OptimisticTransactionDB db,
			StorageImplementationRocksDB store)
	{
		super(
				name,
				columnFamily,
				columnFamilyName,
				keyConverter,
				valueConverter,
				db,
				store);
		this.inverseCFHandle = inverseCFHandle;
		this.inverseCFName = inverseColumnFamilyName;
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
		byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(valueBytes, keyBytes);
		this.store.ensureTransaction(tx -> {
			try
			{
			   tx.put(inverseCFHandle, rocksDBKey, new byte[0]);
			}
			catch (RocksDBException e)
			{
				throw new HGException(e);
			}
		});
	}

	@Override
	public HGRandomAccessResult<IndexKey> findByValue(IndexValue value)
	{
		checkOpen();
		return this.store.ensureTransaction(tx -> {
			var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(valueConverter.toByteArray(value)));
			var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(valueConverter.toByteArray(value)));
			var ro = new ReadOptions()
					.setSnapshot(tx.getSnapshot())
					.setIterateLowerBound(lower)
					.setIterateUpperBound(upper);
			return new IteratorResultSet<IndexKey>(
					tx.getIterator(ro, inverseCFHandle), List.of(lower, upper, ro), false)
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

		});
	}

	@Override
	public IndexKey findFirstByValue(IndexValue value)
	{
		checkOpen();

		byte[] valueBytes = this.valueConverter.toByteArray(value);
		byte[] firstRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(valueBytes);
		byte[] lastRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(valueBytes);

		return this.store.ensureTransaction(tx -> {
			try(
					var lower = new Slice(firstRocksDBKey);
					var upper = new Slice(lastRocksDBKey);
					var ro = new ReadOptions()
							.setSnapshot(tx.getSnapshot())
							.setIterateLowerBound(lower)
							.setIterateUpperBound(upper);
					RocksIterator iterator = tx.getIterator(ro, inverseCFHandle))
			{

				iterator.seekToFirst();

				if (iterator.isValid())
				{
					byte[] bytes = iterator.key();
					var keyBytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
							bytes);
					return keyConverter.fromByteArray(keyBytes, 0,
							keyBytes.length);

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
			/*
			If the iterator is not valid and the
			 */
					return null;
				}
			}
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
}


