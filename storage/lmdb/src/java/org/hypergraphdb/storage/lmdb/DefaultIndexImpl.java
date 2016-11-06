/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.text.MessageFormat;
import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.SearchResultWrapper;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.VanillaTransaction;

import static org.hypergraphdb.storage.lmdb.LMDBUtils.checkArgNotNull;
import com.google.common.primitives.UnsignedBytes;

import static org.fusesource.lmdbjni.Constants.*;

import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.DatabaseConfig;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.OperationStatus;
import org.fusesource.lmdbjni.Transaction;

/**
 * <p>
 * A default index implementation. This implementation works by maintaining a
 * separate DB, using a B-tree, <code>byte []</code> lexicographical ordering on
 * its keys. The keys are therefore assumed to by <code>byte [] </code>
 * instances.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class DefaultIndexImpl<KeyType, ValueType>
		implements HGSortIndex<KeyType, ValueType>
{
	/**
	 * Prefix of HyperGraph index DB filenames.
	 */
	public static final String DB_NAME_PREFIX = "hgstore_idx_";

	protected LmdbStorageImplementation storage;
	protected HGTransactionManager transactionManager;
	protected String name;
	protected Database db;
	protected Database db2;
	private boolean owndb;
	protected Comparator<byte[]> comparator;
	protected boolean sort_duplicates = true;
	protected ByteArrayConverter<KeyType> keyConverter;
	protected ByteArrayConverter<ValueType> valueConverter;

	protected void checkOpen()
	{
		if (!isOpen())
			throw new HGException("Attempting to operate on index '" + name
					+ "' while the index is being closed.");
	}

	protected TransactionLmdbImpl txn()
	{
		HGTransaction tx = transactionManager.getContext().getCurrent();
		if (tx == null
				|| tx.getStorageTransaction() instanceof VanillaTransaction)
			return TransactionLmdbImpl.nullTransaction();
		else
			return (TransactionLmdbImpl) tx.getStorageTransaction();
	}


	boolean isSplitIndex()
	{
		return name.equals("HGATOMTYPE") || name.equals("subgraph.index")
				|| name.equals("revsubgraph.index")
				|| name.equals("type_subgraph.index");
	}

	public DefaultIndexImpl(String indexName, LmdbStorageImplementation storage,
			HGTransactionManager transactionManager,
			ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter,
			Comparator<byte[]> comparator)
	{
		this.name = indexName;
		this.storage = storage;
		this.transactionManager = transactionManager;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		this.comparator = comparator;
		owndb = true;
	}

	public String getName()
	{
		return name;
	}

	public String getDatabaseName()
	{
		return DB_NAME_PREFIX + name;
	}

	public Comparator<byte[]> getComparator()
	{
		Comparator<byte[]> comp = null;

		try
		{
			if (comparator != null)
				return comparator;

			comp = db.getConfig().getComparator();
			if (comp == null)
				comp = UnsignedBytes.lexicographicalComparator();

			return comp;
		}
		catch (LMDBException ex)
		{
			throw new HGException(ex);
		}
	}

	public void open()
	{
		try
		{
			DatabaseConfig dbConfig = storage.getConfiguration()
					.getDatabaseConfig().cloneConfig();
			dbConfig.setDupSort(sort_duplicates);
			if (comparator != null)
			{
				// dbConfig.setComparator(comparator);
				throw new IllegalArgumentException(
						"Custom comparators are not presently supported in LMDB");
			}
			db = storage.getEnvironment().openDatabase(txn().getDbTransaction(),
					DB_NAME_PREFIX + name, dbConfig);

			if (isSplitIndex())
			{
				db2 = storage.getEnvironment().openDatabase(
						txn().getDbTransaction(), DB_NAME_PREFIX + name + "2",
						dbConfig);
			}
		}
		catch (Throwable t)
		{
			throw new HGException("While attempting to open index ;" + name
					+ "': " + t.toString(), t);
		}
	}

	public void close()
	{
		if (db == null || !owndb)
			return;

		try
		{
			db.close();
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
		finally
		{
			db = null;
		}

		if (isSplitIndex())
		{
			try
			{
				db2.close();
			}
			catch (Throwable t)
			{
				throw new HGException(t);
			}
			finally
			{
				db2 = null;
			}
		}
	}

	public boolean isOpen()
	{
		return db != null;
	}

	public HGRandomAccessResult<ValueType> scanValues()
	{
		checkOpen();
		assert !isSplitIndex() : "Not expecting scans on split index";
		HGRandomAccessResult<ValueType> result = null;
		Cursor cursor = null;

		try
		{
			TransactionLmdbImpl tx = txn();
			cursor = db.openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.FIRST);
			if (entry != null)
				result = new KeyRangeForwardResultSet<ValueType>(
						tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), valueConverter);
			else
			{
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public HGRandomAccessResult<KeyType> scanKeys()
	{
		checkOpen();
		assert !isSplitIndex() : "Not expecting scans on split index";
		HGRandomAccessResult<KeyType> result = null;
		Cursor cursor = null;
		try
		{
			TransactionLmdbImpl tx = txn();
			cursor = db.openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.FIRST);
			if (entry != null)
				result = new KeyScanResultSet<KeyType>(tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), keyConverter);
			else
			{
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
				result = (HGRandomAccessResult<KeyType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public void addEntry(KeyType keyType, ValueType value)
	{
		checkOpen();
		if (keyType == null)
		{
			// System.out.println("Keytype is null for:" + getName());
			return;
		}

		// System.err.println("addEntry First for " + name + ",key:" + keyType +
		// ",val:" + value);

		byte[] key = keyConverter.toByteArray(keyType);
		byte[] dbvalue = valueConverter.toByteArray(value);
		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				db.put(txn().getDbTransaction(), key, dbvalue, NODUPDATA);
				break;

			default:
				db2.put(txn().getDbTransaction(), key, dbvalue, NODUPDATA);
				break;
			}

			// System.out.println("Indexput." + key.length + "," +
			// dbvalue.length);
		}
		catch (LMDBException ex)
		{
			String msg = MessageFormat.format(
					"Failed to add entry (key:{0},data:{1}) to index {2}: {3}",
					keyType, value, name, ex.toString());
			throw new HGException(msg, ex);
		}
	}

	public void removeEntry(KeyType keyType, ValueType value)
	{
		checkOpen();
		if (keyType == null)
		{
			// System.out.println("Keytype is null for:" + getName());
			return;
		}

		DatabaseEntry keyEntry = new DatabaseEntry(
				keyConverter.toByteArray(keyType));
		DatabaseEntry valueEntry = new DatabaseEntry(
				valueConverter.toByteArray(value));
		Cursor cursor = null;

		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				cursor = db.openCursor(txn().getDbTransaction());
				break;

			default:
				cursor = db2.openCursor(txn().getDbTransaction());
				break;
			}

			Entry entry = null;

			if (sort_duplicates)
			{
				entry = cursor.get(CursorOp.GET_BOTH, keyEntry.getData(),
						valueEntry.getData());
				if (entry != null)
					cursor.delete();
			}
			else
			{
				OperationStatus status = cursor.get(CursorOp.GET_CURRENT,
						keyEntry, valueEntry);
				if (status == OperationStatus.SUCCESS)
					cursor.delete();
			}
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		finally
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
		}
	}

	public void removeAllEntries(KeyType keyType)
	{
		checkOpen();
		if (keyType == null)
		{
			// System.out.println("Keytype is null for:" + getName());
			return;
		}

		byte[] dbkey = keyConverter.toByteArray(keyType);
		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				db.delete(txn().getDbTransaction(), dbkey);
				break;

			default:
				db2.delete(txn().getDbTransaction(), dbkey);
				break;
			}
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to delete entry from index '" + name
					+ "': " + ex.toString(), ex);
		}
	}

	void ping(Transaction tx)
	{
		byte[] key = new byte[1];
		try
		{
			db.get(tx, key);
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to ping index '" + name + "': " + ex.toString(),
					ex);
		}
	}

	public ValueType getData(KeyType keyType)
	{
		checkOpen();
		byte[] key = keyConverter.toByteArray(keyType);
		byte[] value = null;
		ValueType result = null;

		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				value = db.get(txn().getDbTransaction(), key);
				break;

			default:
				value = db2.get(txn().getDbTransaction(), key);
				break;
			}

			if (value != null)
				result = valueConverter.fromByteArray(value, 0, value.length);
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public ValueType findFirst(KeyType keyType)
	{
		try
		{
			// System.out.println(getStats());
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}

		checkOpen();
		// System.err.println("findFirst for " + name + ",key:" + keyType);
		byte[] key = keyConverter.toByteArray(keyType);
		ValueType result = null;
		Cursor cursor = null;
		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				cursor = db.openCursor(txn().getDbTransaction());
				break;

			default:
				cursor = db2.openCursor(txn().getDbTransaction());
				break;
			}

			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				result = valueConverter.fromByteArray(entry.getValue(), 0,
						entry.getValue().length);
		}
		catch (Exception ex)
		{
			// try to get the db flags to understand why the error is happening.
			int flags = 0;
			try
			{
				flags = db.getFlags(txn().getDbTransaction());
			}
			catch (Exception ex2)
			{
				// do nothing
			}
			String msg = MessageFormat.format(
					"Failed to add lookup index {0} (key:{1}): {2}. Db Flage: {3}",
					keyType, name, ex.toString(), flags);
			throw new HGException(msg, ex);
		}
		finally
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
		}
		return result;
	}

	/**
	 * <p>
	 * Find the last entry, assuming ordered duplicates, corresponding to the
	 * given key.
	 * </p>
	 * 
	 * @param keyType
	 *            The key whose last entry is sought.
	 * @return The last (i.e. greatest, i.e. maximum) data value for that key or
	 *         null if the set of entries for the key is empty.
	 */
	public ValueType findLast(KeyType keyType)
	{
		checkOpen();
		byte[] key = keyConverter.toByteArray(keyType);
		ValueType result = null;
		Cursor cursor = null;
		try
		{
			switch (keyBucket(keyType))
			{
			case 0:
				cursor = db.openCursor(txn().getDbTransaction());
				break;

			default:
				cursor = db2.openCursor(txn().getDbTransaction());
				break;
			}

			Entry entry = cursor.get(CursorOp.LAST, key);
			if (entry != null)
				result = valueConverter.fromByteArray(entry.getValue(), 0,
						entry.getValue().length);
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		finally
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
		}
		return result;
	}

	public HGRandomAccessResult<ValueType> find(KeyType keyType)
	{
		checkOpen();
		checkArgNotNull(keyType, "keyType");

		byte[] key = keyConverter.toByteArray(keyType);
		HGRandomAccessResult<ValueType> result = null;
		Cursor cursor = null;
		try
		{
			TransactionLmdbImpl tx = txn();

			switch (keyBucket(keyType))
			{
			case 0:
				cursor = db.openCursor(tx.getDbTransaction());
				break;

			default:
				cursor = db2.openCursor(tx.getDbTransaction());
				break;
			}

			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				result = new SingleKeyResultSet<ValueType>(
						tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), valueConverter);
			else
			{
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
			System.out.println("Inner Exception:");
			ex.printStackTrace();
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	private HGSearchResult<ValueType> findOrdered(KeyType keyType,
			boolean lower_range, boolean compare_equals)
	{
		checkOpen();
		/*
		 * if (key == null) throw new HGException("Attempting to lookup index '"
		 * + name + "' with a null key.");
		 */
		byte[] keyAsBytes = keyConverter.toByteArray(keyType);
		Cursor cursor = null;

		try
		{
			TransactionLmdbImpl tx = txn();
			switch (keyBucket(keyType))
			{
			case 0:
				cursor = db.openCursor(tx.getDbTransaction());
				break;

			default:
				cursor = db2.openCursor(tx.getDbTransaction());
				break;
			}

			Entry entry = cursor.get(CursorOp.SET_RANGE, keyAsBytes);

			if (entry != null)
			{
				Comparator<byte[]> comparator = getComparator();
				if (!compare_equals) // strict < or >?
				{
					if (lower_range)
						entry = cursor.get(CursorOp.PREV);
					else if (comparator.compare(keyAsBytes,
							entry.getKey()) == 0)
						entry = cursor.get(CursorOp.NEXT_NODUP);
				}
				// Lmdb cursor will position on the key or on the next element
				// greater than the key
				// in the latter case we need to back up by one for < (or <=)
				// query
				else if (lower_range
						&& comparator.compare(keyAsBytes, entry.getKey()) != 0)
					entry = cursor.get(CursorOp.PREV);
			}
			else if (lower_range)
				entry = cursor.get(CursorOp.LAST);
			else
				entry = cursor.get(CursorOp.FIRST);

			if (entry != null)
				if (lower_range)
					return new SearchResultWrapper<ValueType>(
							new KeyRangeBackwardResultSet<ValueType>(
									tx.attachCursor(cursor),
									new DatabaseEntry(entry.getKey()),
									valueConverter));
				else
					return new SearchResultWrapper<ValueType>(
							new KeyRangeForwardResultSet<ValueType>(
									tx.attachCursor(cursor),
									new DatabaseEntry(entry.getKey()),
									valueConverter));
			else
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}

			return (HGSearchResult<ValueType>) HGSearchResult.EMPTY;
		}
		catch (Throwable ex)
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
			System.out.println("Inner Exception:");
			ex.printStackTrace();
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
	}

	public HGSearchResult<ValueType> findGT(KeyType key)
	{
		return findOrdered(key, false, false);
	}

	public HGSearchResult<ValueType> findGTE(KeyType key)
	{
		return findOrdered(key, false, true);
	}

	public HGSearchResult<ValueType> findLT(KeyType key)
	{
		return findOrdered(key, true, false);
	}

	public HGSearchResult<ValueType> findLTE(KeyType key)
	{
		return findOrdered(key, true, true);
	}

	protected void finalize()
	{
		if (isOpen())
			try
			{
				close();
			}
			catch (Throwable t)
			{
			}
	}

	public long count()
	{
		return stats().entries(Long.MAX_VALUE, false).value();
	}

	public long count(KeyType keyType)
	{
		return stats().valuesOfKey(keyType, Long.MAX_VALUE, false).value();
	}

	@Override
	public LMDBIndexStats<KeyType, ValueType> stats()
	{
		return new LMDBIndexStats<KeyType, ValueType>(this);
	}

	int keyBucket(KeyType keyType)
	{
		if (isSplitIndex())
		{
			if (keyType instanceof HGPersistentHandle)
			{
				HGPersistentHandle handle = (HGPersistentHandle) keyType;
				String hdlStr = handle.toString();
				char lastChar = hdlStr.charAt(hdlStr.length() - 1);
				if (lastChar == '1' || lastChar == '3' || lastChar == '5'
						|| lastChar == '7' || lastChar == '9')
				{
					return 1;
				}
			}
			if (keyType instanceof HGPersistentHandle[])
			{
				HGPersistentHandle[] handles = (HGPersistentHandle[]) keyType;
				HGPersistentHandle handle = handles[0];
				String hdlStr = handle.toString();
				char lastChar = hdlStr.charAt(hdlStr.length() - 1);
				if (lastChar == '1' || lastChar == '3' || lastChar == '5'
						|| lastChar == '7' || lastChar == '9')
				{
					return 1;
				}
			}
		}
		return 0;
	}

}
