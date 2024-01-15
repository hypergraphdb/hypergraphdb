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
import org.hypergraphdb.storage.rocksdb.iterate.DistinctValueIterator;
import org.hypergraphdb.storage.rocksdb.iterate.ValueIterator;
import org.rocksdb.*;

import java.util.Arrays;
import java.util.List;

/**
 * Fixed key - fixed value multivalued DB
 */
public class FKFVMVDB extends LogicalDB
{
	public FKFVMVDB(String name, ColumnFamilyHandle cfHandle,
			ColumnFamilyOptions cfOptions)
	{
		super(name, cfHandle, cfOptions);
	}

	@Override
	public void put(Transaction tx, byte[] key, byte[] value)
	{
		try
		{
			tx.put(this.columnFamilyHandle,
					FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(
							key, value), new byte[0]);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public byte[] get(Transaction tx, byte[] key)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(Transaction tx, byte[] key)
	{
		try (var first = new Slice(
				FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						key)); var last = new Slice(
				FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						key));

			 var iteratorReadOptions = new ReadOptions().setIterateLowerBound(
							 first).setIterateUpperBound(last)
					 .setSnapshot(tx.getSnapshot());

			 RocksIterator iterator = tx.getIterator(iteratorReadOptions,
					 this.columnFamilyHandle))
		{
			while (iterator.isValid())
			{
				iterator.next();
				byte[] next = iterator.key();
				try
				{
					tx.delete(this.columnFamilyHandle, next);
				}
				catch (RocksDBException e)
				{
					throw new HGException(e);
				}
			}
			try
			{
				iterator.status();
			}
			catch (RocksDBException e)
			{
				throw new HGException(e);
			}
		}
	}

	@Override
	public ValueIterator<byte[]> iterateValuesForKey(Transaction tx,
			byte[] byteArray)
	{
		var lower = new Slice(
				FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						byteArray));
		var upper = new Slice(
				FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						byteArray));
		var ro = new ReadOptions().setSnapshot(tx.getSnapshot())
				.setIterateLowerBound(lower).setIterateUpperBound(upper);

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> FixedKeyFixedValueColumnFamilyMultivaluedDB.extractValue(
						key),
				bytes -> FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(
						byteArray, bytes), List.of(lower, upper, ro));
	}

	@Override
	public void delete(Transaction tx, byte[] localKey, byte[] value)
	{
		var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(
				localKey, value);
		try
		{
			tx.delete(this.columnFamilyHandle, rocksDBkey);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public void printDB()
	{
		throw new UnsupportedOperationException();
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
		var i = tx.getIterator(new ReadOptions(), this.columnFamilyHandle);
		i.seekToFirst();

		while (i.isValid())
		{
			var k = i.key();
			var v = i.value();
			System.out.println();
			var parserdKey = Arrays.copyOfRange(k, 0, 16);
			var parsedValue = new String(v);
			System.out.printf("primitive; key(%s); value(%s) %n", parserdKey,
					parsedValue);

			//primitive
			i.next();
		}
		try
		{
			i.status();
		}
		catch (RocksDBException e)
		{
			System.out.println("iterator failed with an exception");
			throw new RuntimeException(e);
		}
		System.out.println("iterated the database successfully");

	}
}
