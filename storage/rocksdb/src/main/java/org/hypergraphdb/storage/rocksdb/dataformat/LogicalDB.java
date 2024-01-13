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

/*
 * TODO
 *  consider creating a common API for the different logical databases
 *  i.e. multivalued  db / fixed key size / var key size etc.
 */

/**
 *
 * A logical database which is backed by a column family in rocks db
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

	public static class VKVVMVDB extends LogicalDB
	{
		public VKVVMVDB(String name, ColumnFamilyHandle cfHandle, ColumnFamilyOptions cfOptions)
		{
			super(name, cfHandle, cfOptions);
		}

		@Override
		public void put(Transaction tx, byte[] key, byte[] value)
		{
			byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(key, value);
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
		public byte[] get(Transaction tx,
				byte[] key)
		{
			byte[] firstRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(key);
			byte[] lastRocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(key);
			try (var lower = new Slice(firstRocksDBKey);
				 var upper = new Slice(lastRocksDBKey);
				 var ro = new ReadOptions()
						 .setSnapshot(tx.getSnapshot())
						 .setIterateLowerBound(lower)
						 .setIterateUpperBound(upper);
				 RocksIterator iterator = tx.getIterator(ro, this.columnFamilyHandle);
			)
			{
				iterator.seekToFirst();

				if (iterator.isValid())
				{
					byte[] bytes = iterator.key();
					var valuebytes = VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(bytes);
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

			try (
					var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(key));
					var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(key));
					var ro = new ReadOptions()
							.setSnapshot(tx.getSnapshot())
							.setIterateLowerBound(lower)
							.setIterateUpperBound(upper);
					RocksIterator iterator  = tx.getIterator(ro, this.columnFamilyHandle))
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
		public ValueIterator<byte[]> iterateValuesForKey(
				Transaction tx,
				byte[] keyBytes)
		{
			var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(keyBytes));
			var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(keyBytes));
			var ro = new ReadOptions()
					.setSnapshot(tx.getSnapshot())
					.setIterateLowerBound(lower)
					.setIterateUpperBound(upper);

			return new ValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(value) -> VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(keyBytes, value),
					List.of(ro));
		}

		@Override
		public void delete(Transaction tx, byte[] localKey, byte[] value)
		{
			byte[] rocksDBKey = VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
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
			var ro = new ReadOptions()
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractKey(key),
					VarKeyVarValueColumnFamilyMultivaluedDB::firstRocksDBKey,
					List.of(ro));
		}

		@Override
		public ValueIterator<byte[]> iterateValues(Transaction tx)
		{
			var ro = new ReadOptions()
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(valuebytes) -> { throw new HGException("Cannot move the cursor only with value"); },
					List.of(ro));
		}

		@Override
		public ValueIterator<byte[]> iterateLT(Transaction tx, byte[] byteArray)
		{
			var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(byteArray));
			var ro = new ReadOptions()
					.setIterateUpperBound(upper)
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(valuebytes) -> { throw new HGException("Cannot move the cursor only with value"); },
					List.of(ro));
		}

		@Override
		public ValueIterator<byte[]> iterateLTE(Transaction tx, byte[] byteArray)
		{
			var upper = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(byteArray));
			var ro = new ReadOptions()
					.setIterateUpperBound(upper)
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(valuebytes) -> { throw new HGException("Cannot move the cursor only with value"); },
					List.of(ro));
		}

		@Override
		public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
		{
			var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.lastRocksDBKey(byteArray));
			var ro = new ReadOptions()
					.setIterateLowerBound(lower)
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(valuebytes) -> { throw new HGException("Cannot move the cursor only with value"); },
					List.of(ro));
		}

		@Override
		public ValueIterator<byte[]> iterateGTE(Transaction tx, byte[] byteArray)
		{
			var lower = new Slice(VarKeyVarValueColumnFamilyMultivaluedDB.firstRocksDBKey(byteArray));
			var ro = new ReadOptions()
					.setIterateLowerBound(lower)
					.setSnapshot(tx.getSnapshot());

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> VarKeyVarValueColumnFamilyMultivaluedDB.extractValue(key),
					(valuebytes) -> { throw new HGException("Cannot move the cursor only with value"); },
					List.of(ro));
		}

		@Override
		public void printDB(Transaction tx)
		{
			throw new UnsupportedOperationException();
		}

	}
	public static class FKFVMVDB extends LogicalDB
	{
		public FKFVMVDB(String name, ColumnFamilyHandle cfHandle, ColumnFamilyOptions cfOptions)
		{
			super(name, cfHandle, cfOptions);
		}

		@Override
		public void put(Transaction tx, byte[] key, byte[] value)
		{
			try
			{
				tx.put(this.columnFamilyHandle, FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(key, value), new byte[0]);
			}
			catch (RocksDBException e)
			{
				throw new HGException(e);
			}
		}

		@Override
		public byte[] get(Transaction tx,
				byte[] key)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void delete(Transaction tx, byte[] key)
		{
			try (
					var first = new Slice(
							FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(key));
					var last = new Slice(
							FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(key));

					var iteratorReadOptions = new ReadOptions()
							.setIterateLowerBound(first)
							.setIterateUpperBound(last)
							.setSnapshot(tx.getSnapshot());

					RocksIterator iterator = tx.getIterator(
							iteratorReadOptions,
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
			var lower =  new Slice(
					FixedKeyFixedValueColumnFamilyMultivaluedDB.firstRocksDBKey(byteArray));
			var upper = new Slice(FixedKeyFixedValueColumnFamilyMultivaluedDB.lastRocksDBKey(byteArray));
			var ro = new ReadOptions()
					.setSnapshot(tx.getSnapshot())
					.setIterateLowerBound(lower)
					.setIterateUpperBound(upper);

			return new DistinctValueIterator<>(
					tx.getIterator(ro, this.columnFamilyHandle),
					(key, value) -> FixedKeyFixedValueColumnFamilyMultivaluedDB.extractValue(key),
					bytes -> FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(byteArray, bytes),
					List.of(lower, upper, ro));
		}

		@Override
		public void delete(Transaction tx, byte[] localKey, byte[] value)
		{
			var rocksDBkey = FixedKeyFixedValueColumnFamilyMultivaluedDB.makeRocksDBKey(localKey, value);
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
		public ValueIterator<byte[]> iterateLTE(Transaction tx,
				byte[] byteArray)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public ValueIterator<byte[]> iterateGTE(Transaction tx,
				byte[] byteArray)
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
				System.out.printf("primitive; key(%s); value(%s) %n", parserdKey, parsedValue);

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
	public static class SVDB extends LogicalDB
	{

		public SVDB(String name, ColumnFamilyHandle cfHandle, ColumnFamilyOptions cfOptions)
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
		public byte[] get(Transaction tx,
				byte[] key)
		{
			try(var ro = new ReadOptions().setSnapshot(tx.getSnapshot()))
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
		public ValueIterator<byte[]> iterateLTE(Transaction tx,
				byte[] byteArray)
		{
			throw new IllegalStateException();
		}

		@Override
		public ValueIterator<byte[]> iterateGT(Transaction tx, byte[] byteArray)
		{
			throw new IllegalStateException();
		}

		@Override
		public ValueIterator<byte[]> iterateGTE(Transaction tx,
				byte[] byteArray)
		{
			throw new IllegalStateException();
		}

		@Override
		public void printDB(Transaction tx)
		{
			throw new IllegalStateException();
		}
	}
}
