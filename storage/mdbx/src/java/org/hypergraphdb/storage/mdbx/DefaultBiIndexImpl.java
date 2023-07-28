/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage.mdbx;

import com.castortech.mdbxjni.CursorOp;
import com.castortech.mdbxjni.DatabaseEntry;
import com.castortech.mdbxjni.Entry;
import com.castortech.mdbxjni.MDBXException;
import com.castortech.mdbxjni.SecondaryCursor;
import com.castortech.mdbxjni.SecondaryDatabase;
import com.castortech.mdbxjni.SecondaryDbConfig;
import com.castortech.mdbxjni.JNI.MDBX_stat;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionIsReadonlyException;
import org.hypergraphdb.util.HGUtils;

@SuppressWarnings("unchecked")
public class DefaultBiIndexImpl<KeyType, ValueType>
		extends DefaultIndexImpl<KeyType, ValueType>
		implements HGBidirectionalIndex<KeyType, ValueType>
{
	private static final String SECONDARY_DB_NAME_PREFIX = DB_NAME_PREFIX
			+ "_secondary";
	private SecondaryDatabase secondaryDb = null;

	public DefaultBiIndexImpl(String indexName,
							  MdbxStorageImplementation storage,
							  HGTransactionManager transactionManager,
							  ByteArrayConverter<KeyType> indexKeyConverter,
							  ByteArrayConverter<ValueType> indexValueConverter)
	{
		super(indexName, storage, transactionManager, indexKeyConverter, indexValueConverter);
	}

	@Override
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
		catch (MDBXException me)
		{
			if (me.getErrorCode() == 5)
			{ // 5 is access denied that is thrown if the transaction is
				// read-only
				throw new TransactionIsReadonlyException();
			}
		}
		catch (Exception e)
		{
			throw new HGException("While attempting to open index " + name
					+ "': " + e.toString(), e);
		}
	}

	@Override
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

	@Override
	public boolean isOpen()
	{
		return super.isOpen() && secondaryDb != null;
	}

	@Override
	public void addEntry(KeyType key, ValueType value)
	{
		checkOpen();
//	System.err.println("addEntry First for " + name + ",key:" + key + ",val:" + value);
		byte[] dbkey = indexKeyConverter.toByteArray(key);
		byte[] dbvalue = indexValueConverter.toByteArray(value);

		try
		{
			db.put(txn().getDbTransaction(), dbkey, dbvalue);
//		System.out.println("IndexPut." + dbkey.length + "," + dbvalue.length);
		}
		catch (MDBXException ex)
		{
			throw new HGException("Failed to add entry to index '" + name
					+ "': " + ex.toString(), ex);
		}
	}

	@Override
	public HGRandomAccessResult<KeyType> findByValue(ValueType value)
	{
		if (!isOpen())
			throw new HGException("Attempting to lookup index '" + name
					+ "' while it is closed.");

		byte[] dbkey = indexValueConverter.toByteArray(value);
		HGRandomAccessResult<KeyType> result = null;
		SecondaryCursor cursor = null;

		try
		{
			TransactionMdbxImpl tx = txn();
			cursor = secondaryDb.openSecondaryCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, dbkey);
			if (entry != null)
				result = new SingleValueResultSet<>(tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), indexKeyConverter);
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<KeyType>) HGSearchResult.EMPTY;
			}
		}
		catch (Exception ex)
		{
			HGUtils.closeNoException(cursor);
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	@Override
	public KeyType findFirstByValue(ValueType value)
	{
		if (!isOpen())
			throw new HGException("Attempting to lookup by value index '" + name
					+ "' while it is closed.");

		byte[] key = indexValueConverter.toByteArray(value);
		KeyType result = null;
		SecondaryCursor cursor = null;
		try
		{
			cursor = secondaryDb.openSecondaryCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				result = indexKeyConverter.fromByteArray(entry.getValue(), 0, entry.getValue().length);
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		finally
		{
			HGUtils.closeNoException(cursor);
		}
		return result;
	}

	@Override
	public long countKeys(ValueType value)
	{
		byte[] key = indexValueConverter.toByteArray(value);
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
		catch (MDBXException ex)
		{
			throw new HGException(ex);
		}
		finally
		{
			HGUtils.closeNoException(cursor);
		}
	}

	@Override
	public String getDatabaseName()
	{
		return SECONDARY_DB_NAME_PREFIX + name;
	}

	public String getPrimaryDatabaseName()
	{
		return super.getDatabaseName();
	}

	@Override
	public String getStats()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(super.getStats());

		MDBX_stat stat = db.stat(txn().getDbTransaction());
		sb.append("Secondary Index " + getName() + " Stats:");
		sb.append(stat.toString());
		sb.append('\n');

		return sb.toString();
	}
}