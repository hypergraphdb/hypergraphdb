/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.util.Comparator;

import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.SecondaryCursor;
import org.fusesource.lmdbjni.SecondaryDatabase;
import org.fusesource.lmdbjni.SecondaryDbConfig;
import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.transaction.HGTransactionManager;

@SuppressWarnings("unchecked")
public class DefaultBiIndexImpl<KeyType, ValueType>
		extends DefaultIndexImpl<KeyType, ValueType>
		implements HGBidirectionalIndex<KeyType, ValueType>
{
	private static final String SECONDARY_DB_NAME_PREFIX = DB_NAME_PREFIX
			+ "_secondary";
	SecondaryDatabase secondaryDb = null;

	public DefaultBiIndexImpl(String indexName,
			LmdbStorageImplementation storage,
			HGTransactionManager transactionManager,
			ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter,
			Comparator<byte[]> comparator)
	{
		super(indexName, storage, transactionManager, keyConverter,
				valueConverter, comparator);
	}

	public void open()
	{
		sort_duplicates = false;
		super.open();
		try
		{
			SecondaryDbConfig dbConfig = new SecondaryDbConfig();
			dbConfig.setCreate(true);
			dbConfig.setDupSort(true);
			secondaryDb = storage.getEnvironment().openSecondaryDatabase(
					txn().getDbTransaction(), db,
					SECONDARY_DB_NAME_PREFIX + name, dbConfig);
		}
		catch (Throwable t)
		{
			throw new HGException("While attempting to open index ;" + name
					+ "': " + t.toString(), t);
		}
	}

	public void close()
	{
		HGException exception = null;

		try
		{
			super.close();
		}
		catch (HGException ex)
		{
			exception = ex;
		}

		if (secondaryDb == null)
			return;

		// Attempt to close secondary database even if there was an exception
		// during the close of the primary.
		try
		{
			secondaryDb.close();
		}
		catch (Throwable t)
		{
			if (exception == null)
				exception = new HGException(t);
		}
		finally
		{
			secondaryDb = null;
		}

		if (exception != null)
			throw exception;
	}

	public boolean isOpen()
	{
		return super.isOpen() && secondaryDb != null;
	}

	public void addEntry(KeyType key, ValueType value)
	{
		checkOpen();
		// System.err.println("addEntry First for " + name + ",key:" + key +
		// ",val:" + value);
		byte[] dbkey = keyConverter.toByteArray(key);
		byte[] dbvalue = valueConverter.toByteArray(value);
		try
		{
			db.put(txn().getDbTransaction(), dbkey, dbvalue);
			// System.out.println("IndexPut." + dbkey.length + "," +
			// dbvalue.length);

		}
		catch (LMDBException ex)
		{
			throw new HGException("Failed to add entry to index '" + name
					+ "': " + ex.toString(), ex);
		}
	}

	public HGRandomAccessResult<KeyType> findByValue(ValueType value)
	{
		if (!isOpen())
			throw new HGException("Attempting to lookup index '" + name
					+ "' while it is closed.");
		/*
		 * if (value == null) throw new HGException(
		 * "Attempting to lookup index '" + name + "' with a null key.");
		 */
		byte[] dbkey = valueConverter.toByteArray(value);
		HGRandomAccessResult<KeyType> result = null;
		SecondaryCursor cursor = null;
		try
		{
			TransactionLmdbImpl tx = txn();
			cursor = secondaryDb.openSecondaryCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, dbkey);
			if (entry != null)
				result = new SingleValueResultSet<KeyType>(
						tx.attachCursor(cursor),
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
		catch (Exception ex)
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

	public KeyType findFirstByValue(ValueType value)
	{
		if (!isOpen())
			throw new HGException("Attempting to lookup by value index '" + name
					+ "' while it is closed.");
		/*
		 * if (value == null) throw new HGException(
		 * "Attempting to lookup by value index '" + name +
		 * "' with a null value.");
		 */
		byte[] key = valueConverter.toByteArray(value);
		KeyType result = null;
		SecondaryCursor cursor = null;
		try
		{
			cursor = secondaryDb.openSecondaryCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				result = keyConverter.fromByteArray(entry.getKey(), 0,
						entry.getKey().length);
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

	public long countKeys(ValueType value)
	{
		byte[] key = valueConverter.toByteArray(value);
		SecondaryCursor cursor = null;
		try
		{
			cursor = secondaryDb.openSecondaryCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				return cursor.count();
			else
				return 0;
		}
		catch (LMDBException ex)
		{
			throw new HGException(ex);
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
}
