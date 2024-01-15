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

import java.util.List;

/**
 * Var length key var length value multivalued DB
 */
public class VKVVMVDB extends LogicalDB
{
	public VKVVMVDB(String name, ColumnFamilyHandle cfHandle,
			ColumnFamilyOptions cfOptions)
	{
		super(name, cfHandle, cfOptions);
	}

	@Override
	public void put(Transaction tx, byte[] key, byte[] value)
	{
		byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(
				key, value);
		try
		{
			tx.put(this.columnFamilyHandle, rocksDBKey, new byte[0]);
		}
		catch (RocksDBException e)
		{
			throw new HGException(e);
		}
	}

	@Override
	public byte[] get(Transaction tx, byte[] key)
	{
		byte[] firstRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
				key);
		byte[] lastRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(
				key);
		try (var lower = new Slice(firstRocksDBKey); var upper = new Slice(
				lastRocksDBKey); var ro = new ReadOptions().setSnapshot(
						tx.getSnapshot()).setIterateLowerBound(lower)
				.setIterateUpperBound(
						upper); RocksIterator iterator = tx.getIterator(ro,
				this.columnFamilyHandle);)
		{
			iterator.seekToFirst();

			if (iterator.isValid())
			{
				byte[] bytes = iterator.key();
				var valuebytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						bytes);
				return valuebytes;
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
				return null;
			}
		}
	}

	@Override
	public void delete(Transaction tx, byte[] key)
	{

		/*
			TODO consider using RangeDelete which is not yet supported by transactions
		 */

		//        try
		//        {
		//            db.deleteRange(
		//                    columnFamily,
		//                    VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyConverter.toByteArray(key)),
		//                    VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyConverter.toByteArray(key)));
		//        }
		//        catch (RocksDBException e)
		//        {
		//            throw new HGException(e);
		//        }

		try (var lower = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						key)); var upper = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						key)); var ro = new ReadOptions().setSnapshot(
						tx.getSnapshot()).setIterateLowerBound(lower)
				.setIterateUpperBound(
						upper); RocksIterator iterator = tx.getIterator(ro,
				this.columnFamilyHandle))
		{
			iterator.seekToFirst();
			while (iterator.isValid())
			{
				byte[] next = iterator.key();
				try
				{
					tx.delete(this.columnFamilyHandle, next);
				}
				catch (RocksDBException e)
				{
					throw new HGException(e);
				}
				iterator.next();
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
			byte[] keyBytes)
	{
		var lower = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						keyBytes));
		var upper = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						keyBytes));
		var ro = new ReadOptions().setSnapshot(tx.getSnapshot())
				.setIterateLowerBound(lower).setIterateUpperBound(upper);

		return new ValueIterator<>(tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key),
				(value) -> VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(
						keyBytes, value), List.of(ro));
	}

	@Override
	public void delete(Transaction tx, byte[] localKey, byte[] value)
	{
		byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(
				localKey, value);
		try
		{
			tx.delete(this.columnFamilyHandle, rocksDBKey);
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
		var ro = new ReadOptions().setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractKey(
						key),
				VarKeyVarValueColumnFamilyMultivaluedDB::firstRocksDBKey,
				List.of(ro));
	}

	@Override
	public ValueIterator<byte[]> iterateValues(Transaction tx)
	{
		var ro = new ReadOptions().setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key), (valuebytes) -> {
			throw new HGException("Cannot move the cursor only with value");
		}, List.of(ro));
	}

	@Override
	public ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray)
	{
		var upper = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						byteArray));
		var ro = new ReadOptions().setIterateUpperBound(upper)
				.setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key), (valuebytes) -> {
			throw new HGException("Cannot move the cursor only with value");
		}, List.of(ro));
	}

	@Override
	public ValueIterator<byte[]> iterateLTE(Transaction tx, byte[] byteArray)
	{
		var upper = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						byteArray));
		var ro = new ReadOptions().setIterateUpperBound(upper)
				.setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key), (valuebytes) -> {
			throw new HGException("Cannot move the cursor only with value");
		}, List.of(ro));
	}

	@Override
	public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
	{
		var lower = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(
						byteArray));
		var ro = new ReadOptions().setIterateLowerBound(lower)
				.setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key), (valuebytes) -> {
			throw new HGException("Cannot move the cursor only with value");
		}, List.of(ro));
	}

	@Override
	public ValueIterator<byte[]> iterateGTE(Transaction tx, byte[] byteArray)
	{
		var lower = new Slice(
				VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(
						byteArray));
		var ro = new ReadOptions().setIterateLowerBound(lower)
				.setSnapshot(tx.getSnapshot());

		return new DistinctValueIterator<>(
				tx.getIterator(ro, this.columnFamilyHandle),
				(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(
						key), (valuebytes) -> {
			throw new HGException("Cannot move the cursor only with value");
		}, List.of(ro));
	}

	@Override
	public void printDB(Transaction tx)
	{
		throw new UnsupportedOperationException();
	}

}
