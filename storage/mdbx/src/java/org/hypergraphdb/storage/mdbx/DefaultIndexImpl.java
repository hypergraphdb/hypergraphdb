/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.	All rights reserved.
 */
package org.hypergraphdb.storage.mdbx;

import java.text.MessageFormat;
import java.util.Comparator;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGIndexStats;
import org.hypergraphdb.storage.SearchResultWrapper;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionIsReadonlyException;
import org.hypergraphdb.transaction.VanillaTransaction;
import org.hypergraphdb.util.HGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hypergraphdb.storage.mdbx.MDBXUtils.*;
import com.google.common.primitives.UnsignedBytes;

import static com.castortech.mdbxjni.Constants.*;

import com.castortech.mdbxjni.CursorOp;
import com.castortech.mdbxjni.Database;
import com.castortech.mdbxjni.DatabaseConfig;
import com.castortech.mdbxjni.DatabaseEntry;
import com.castortech.mdbxjni.MDBXException;
import com.castortech.mdbxjni.Cursor;
import com.castortech.mdbxjni.Entry;
import com.castortech.mdbxjni.OperationStatus;
import com.castortech.mdbxjni.Transaction;
import com.castortech.mdbxjni.JNI.MDBX_stat;

/**
 * <p>
 * A default index implementation. This implementation works by maintaining a
 * separate DB, using a B-tree, <code>byte []</code> lexicographical ordering on
 * its keys. The keys are therefore assumed to by <code>byte [] </code>
 * instances.
 * </p>
 *
 * @author Borislav Iordanov
 * @author Alain Picard
 */
@SuppressWarnings("unchecked")
public class DefaultIndexImpl<KeyType, ValueType> implements HGSortIndex<KeyType, ValueType>
{
	private static final Logger log = LoggerFactory
			.getLogger(DefaultIndexImpl.class);

	private static final String FAILED_LOOKUP_INDEX = "Failed to lookup index '{}': {}";
	private static final String KEY_TYPE = "keyType";
	private static final String KEYTYPE_NULL = "Keytype is null for:{}";

	/**
	 * Prefix of HyperGraph index DB filenames.
	 */
	public static final String DB_NAME_PREFIX = "hgstore_idx_";

	private boolean owndb;

	protected boolean sort_duplicates = true;
	protected MdbxStorageImplementation storage;
	protected HGTransactionManager transactionManager;
	protected String name;
	protected Database db;
	protected Database db2;
//	protected HGIndexConfig<KeyType, ValueType> indexConfig;
//	protected EncryptUtils encryptUtils;
	protected ByteArrayConverter<KeyType> indexKeyConverter;
	protected ByteArrayConverter<ValueType> indexValueConverter;

	public DefaultIndexImpl(String indexName, 
							MdbxStorageImplementation storage,
							HGTransactionManager transactionManager,
							ByteArrayConverter<KeyType> indexKeyConverter,
							ByteArrayConverter<ValueType> indexValueConverter)
	{
		name = indexName;
		this.storage = storage;
		this.transactionManager = transactionManager;
		this.indexKeyConverter = indexKeyConverter;
		this.indexValueConverter = indexValueConverter;
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

	protected void checkOpen()
	{
		if (!isOpen())
			throw new HGException(
					"Attempting to operate on index '" + name + "' while the index is being closed.");
	}

	protected TransactionMdbxImpl txn()
	{
		HGTransaction tx = transactionManager.getContext().getCurrent();
		if (tx == null
				|| tx.getStorageTransaction() instanceof VanillaTransaction)
			return TransactionMdbxImpl.nullTransaction();
		else
			return (TransactionMdbxImpl) tx.getStorageTransaction();
	}

	protected Comparator<byte[]> getKeyComparator()
	{
		return getInternComparator();
	}

	protected Comparator<byte[]> getInternComparator()
	{
//		if (indexConfig.getKeyComparator() != null)
//			return indexConfig.getKeyComparator();

		return UnsignedBytes.lexicographicalComparator();
	}

	@Override
	public void open()
	{
		if (isOpen())
			throw new HGException(
					"Attempting to open on index '" + name + "' which is already opened.");

		try
		{
			DatabaseConfig dbConfig = storage.getConfiguration()
					.getDatabaseConfig().cloneConfig();
			dbConfig.setDupSort(sort_duplicates);
			if (getKeyComparator() != null)
			{
				dbConfig.setKeyComparator(getKeyComparator());
			}
			db = storage.getEnvironment().openDatabase(txn().getDbTransaction(),
					DB_NAME_PREFIX + name, dbConfig);
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
			throw new HGException("While attempting to open index ;" + name
					+ "': " + e.toString(), e);
		}
	}

	@Override
	public void close()
	{
		if (db == null || !owndb)
			return;

		try
		{
			db.close();
		}
		catch (Exception e)
		{
			throw new HGException(e);
		}
		finally
		{
			db = null;
		}
	}

	@Override
	public boolean isOpen()
	{
		return db != null;
	}

	@Override
	public HGRandomAccessResult<ValueType> scanValues()
	{
		checkOpen();
		HGRandomAccessResult<ValueType> result = null;
		Cursor cursor = null;
		try
		{
			TransactionMdbxImpl tx = txn();
			cursor = db.openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.FIRST);
			if (entry != null)
			{
				result = new KeyRangeForwardResultSet<>(tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), this.indexValueConverter);
			}
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Exception ex)
		{
			HGUtils.closeNoException(cursor);
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name, ex);
		}
		return result;
	}


	@Override
	public HGRandomAccessResult<KeyType> scanKeys()
	{
		checkOpen();
		HGRandomAccessResult<KeyType> result = null;
		Cursor cursor = null;

		try
		{
			TransactionMdbxImpl tx = txn();
			cursor = db.openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.FIRST);

			if (entry != null)
				result = new KeyScanResultSet<>(tx.attachCursor(cursor),
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
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name + " - " + ex.toString(), ex);
		}
		return result;
	}

	@Override
	public void addEntry(KeyType keyType, ValueType value)
	{
		checkOpen();
		if (keyType == null)
		{
			log.warn(KEYTYPE_NULL, getName());
			return;
		}

		try
		{
			byte[] key = indexKeyConverter.toByteArray(keyType);
			byte[] dbvalue = indexValueConverter.toByteArray(value);
			getDb(keyType).put(txn().getDbTransaction(), key, dbvalue,
					NODUPDATA);
		}
		catch (Exception ex)
		{
			String msg = MessageFormat.format(
					"Failed to add entry (key:{0},data:{1}) to index {2}: {3}",
					keyType, value, name, ex.toString());
			throw new HGException(msg, ex);
		}
	}

	@Override
	public void removeEntry(KeyType keyType, ValueType value)
	{
		checkOpen();
		if (keyType == null)
		{
			log.warn(KEYTYPE_NULL, getName());
			return;
		}

		try (Cursor cursor = getDb(keyType)
				.openCursor(txn().getDbTransaction()))
		{
			DatabaseEntry keyEntry = new DatabaseEntry(
					indexKeyConverter.toByteArray(keyType));
			DatabaseEntry valueEntry = new DatabaseEntry(
					indexValueConverter.toByteArray(value));
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
				OperationStatus status = cursor.get(CursorOp.SET, keyEntry,
						valueEntry);
				if (status == OperationStatus.SUCCESS)
					cursor.delete();
			}
		}
		catch (Exception ex)
		{
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name + " - " + ex.toString(),
					ex);
		}
	}

	@Override
	public void removeAllEntries(KeyType keyType)
	{
		checkOpen();

		if (keyType == null)
		{
			log.warn(KEYTYPE_NULL, getName());
			return;
		}

		try
		{
			byte[] dbkey = indexKeyConverter.toByteArray(keyType);
			getDb(keyType).delete(txn().getDbTransaction(), dbkey);
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

//	@Override
//	public ValueType getData(KeyType keyType)
//	{
//		checkOpen();
//		checkArgNotNull(keyType, KEY_TYPE);
//
//		ValueType result = null;
//
//		try
//		{
//			byte[] key = indexKeyConverter.toByteArray(keyType);
//			byte[] value = null;
//			value = getDb(keyType).get(txn().getDbTransaction(), key);
//
//			if (value != null)
//				result = indexValueConverter.fromByteArray(value);
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(
//					ctString.format(FAILED_LOOKUP_INDEX, name, ex.toString()),
//					ex);
//		}
//		return result;
//	}

	@Override
	public ValueType findFirst(KeyType keyType)
	{
		checkOpen();
		checkArgNotNull(keyType, KEY_TYPE);

		ValueType result = null;

		try (Cursor cursor = getDb(keyType).openCursor(txn().getDbTransaction()))
		{
			byte[] key = indexKeyConverter.toByteArray(keyType);
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
				result = indexValueConverter.fromByteArray(entry.getValue(), 0, entry.getValue().length);
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
					keyType, name, ex, flags);
			throw new HGException(msg, ex);
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
		ValueType result = null;

		try (Cursor cursor = getDb(keyType).openCursor(txn().getDbTransaction()))
		{
			byte[] key = indexKeyConverter.toByteArray(keyType);
			Entry entry = cursor.get(CursorOp.LAST, key);

			if (entry != null)
			{
				result = indexValueConverter.fromByteArray(entry.getValue(), 0, entry.getValue().length);
			}
		}
		catch (Exception ex)
		{
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name + ":" + ex.toString(), ex);
		}
		return result;
	}

	@Override
	public HGRandomAccessResult<ValueType> find(KeyType keyType)
	{
		checkOpen();
		checkArgNotNull(keyType, KEY_TYPE);

		HGRandomAccessResult<ValueType> result = null;
		Cursor cursor = null;
		try
		{
			byte[] key = indexKeyConverter.toByteArray(keyType);
			TransactionMdbxImpl tx = txn();
			cursor = getDb(keyType).openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, key);

			if (entry != null)
			{
				result = new SingleKeyResultSet<>(tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), indexValueConverter);
			}
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Exception ex)
		{
			HGUtils.closeNoException(cursor);
			log.info("Inner Exception:", ex);
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name + ":" + ex.toString(), ex);
		}
		return result;
	}

	@SuppressWarnings("resource")
	private HGSearchResult<ValueType> findOrdered(KeyType keyType,
												  boolean lower_range, 
												  boolean compare_equals)
	{
		checkOpen();
		checkArgNotNull(keyType, KEY_TYPE);

		/*
		 * if (key == null) throw new HGException("Attempting to lookup index '"
		 * + name + "' with a null key.");
		 */
		Cursor cursor = null;
		try
		{
			TransactionMdbxImpl tx = txn();
			byte[] keyAsBytes = indexKeyConverter.toByteArray(keyType);
			cursor = getDb(keyType).openCursor(tx.getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET_RANGE, keyAsBytes);

			if (entry != null)
			{
				Comparator<byte[]> comparator = getKeyComparator();
				if (!compare_equals) // strict < or >?
				{
					if (lower_range)
						entry = cursor.get(CursorOp.PREV);
					else if (comparator.compare(keyAsBytes,
							entry.getKey()) == 0)
						entry = cursor.get(CursorOp.NEXT_NODUP);
				}
				// mdbx cursor will position on the key or on the next element
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
			{
				if (lower_range)
				{
					return new SearchResultWrapper<ValueType>(
							new KeyRangeBackwardResultSet<ValueType>(
									tx.attachCursor(cursor),
									new DatabaseEntry(entry.getKey()),
									indexValueConverter));
				}
				else
				{
					return new SearchResultWrapper<ValueType>(
							new KeyRangeForwardResultSet<ValueType>(
									tx.attachCursor(cursor),
									new DatabaseEntry(entry.getKey()),
									indexValueConverter));
				}
			}
			else
			{
				HGUtils.closeNoException(cursor);
				return (HGSearchResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Exception ex)
		{
			HGUtils.closeNoException(cursor);
			log.info("Inner Exception:", ex);
			throw new HGException(FAILED_LOOKUP_INDEX + " " + name + ":" + ex.toString(), ex);
		}
	}

	@Override
	public HGSearchResult<ValueType> findGT(KeyType key)
	{
		return findOrdered(key, false, false);
	}

	@Override
	public HGSearchResult<ValueType> findGTE(KeyType key)
	{
		return findOrdered(key, false, true);
	}

	@Override
	public HGSearchResult<ValueType> findLT(KeyType key)
	{
		return findOrdered(key, true, false);
	}

	@Override
	public HGSearchResult<ValueType> findLTE(KeyType key)
	{
		return findOrdered(key, true, true);
	}

	@Override
	@Deprecated
	protected void finalize()
	{
		if (isOpen())
			try
			{
				close();
			}
			catch (Exception e)
			{
				/* ignore */ }
	}

	@Override
	public long count()
	{
		try
		{
			checkOpen();
			@SuppressWarnings("resource")
			long cnt = db.stat(txn().getDbTransaction()).ms_entries;

			return cnt;
		}
		catch (MDBXException ex)
		{
			throw new HGException(ex);
		}
	}


	@Override
	public long count(KeyType keyType)
	{
		checkOpen();
		try (Cursor cursor = getDb(keyType)
				.openCursor(txn().getDbTransaction()))
		{
			byte[] key = indexKeyConverter.toByteArray(keyType);
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry != null)
			{
				return cursor.count();
			}
			else
				return 0;
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
	}
//
//	public long runInClientContext(IndexResultSet<KeyType> rs, byte[] keyBytes)
//	{
//		KeyType next = rs.next();
//		long result = 0;
//
//		if (TagUtils.keyMatches(keyBytes,
//				indexKeyConverter.toByteArray(next, ConverterMode.STANDARD)))
//		{
//			byte[] keyByteArray = rs.currentKeyByteArray();
//			int clientId = TagUtils.extractClientId(keyByteArray);
//
//			boolean currentTagClient = false;
//			String currentClientIdStr = null;
//
//			HGTransaction tx = transactionManager.getContext().getCurrent();
//			if (tx != null && !(tx
//					.getStorageTransaction() instanceof VanillaTransaction))
//			{
//				currentTagClient = Boolean.valueOf(
//						tx.getContextMap().get(HGConstants.TAG_CLIENT));
//				currentClientIdStr = tx.getContextMap()
//						.get(HGConstants.CLIENT_ID);
//
//				tx.getContextMap().put(HGConstants.TAG_CLIENT,
//						Boolean.TRUE.toString());
//				tx.getContextMap().put(HGConstants.CLIENT_ID,
//						Integer.toString(clientId));
//			}
//
//			try
//			{
//				result = count(next);
//			}
//			finally
//			{
//				if (tx != null && !(tx
//						.getStorageTransaction() instanceof VanillaTransaction))
//				{
//					tx.getContextMap().put(HGConstants.TAG_CLIENT,
//							Boolean.toString(currentTagClient));
//					tx.getContextMap().put(HGConstants.CLIENT_ID,
//							currentClientIdStr);
//				}
//			}
//		}
//
//		return result;
//	}

//	@Override
//	public long countAll(KeyType keyType)
//	{
//		if (indexConfig.getKeyMode() == ConverterMode.STANDARD)
//		{
//			return count(keyType);
//		}
//		else
//		{
//			return countAllTagged(keyType);
//		}
//	}

//	private long countAllTagged(KeyType keyType)
//	{
//		try (HGSearchResult<KeyType> rs = scanAllKeys())
//		{
//			long result = 0;
//			byte[] keyBytes = indexKeyConverter.toByteArray(keyType,
//					ConverterMode.STANDARD);
//
//			while (rs.hasNext())
//			{
//				result += runInClientContext((IndexResultSet<KeyType>) rs,
//						keyBytes);
//			}
//			return result;
//		}
//	}

	public String getStats()
	{
		StringBuilder sb = new StringBuilder();

		MDBX_stat stat = db.stat(txn().getDbTransaction());
		sb.append("Index " + getName() + " Stats:");
		sb.append(stat.toString());
		sb.append('\n');

		return sb.toString();
	}

	private Database getDb(KeyType keyType)
	{
		return db;
	}

//	private int keyBucket(KeyType keyType)
//	{
//		checkArgNotNull(keyType, KEY_TYPE);
//
//		if (isSplitIndex)
//		{
//			HGPersistentHandle handle = null;
//
//			if (keyType instanceof HGPersistentHandle)
//			{
//				handle = (HGPersistentHandle) keyType;
//			}
//			else if (keyType instanceof HGPersistentHandle[])
//			{
//				HGPersistentHandle[] handles = (HGPersistentHandle[]) keyType;
//				handle = handles[0];
//			}
//
//			if (handle != null)
//			{
//				String hdlStr = handle.toStringValue();
//				char lastChar = hdlStr.charAt(hdlStr.length() - 1);
//				if (lastChar == '1' || lastChar == '3' || lastChar == '5'
//						|| lastChar == '7' || lastChar == '9')
//				{
//					return 1;
//				}
//			}
//		}
//		return 0;
//	}

//	private boolean isSplitIndex()
//	{
//		return false;
////		return name.equals("HGATOMTYPE") || name.equals("subgraph.index") || name.equals("revsubgraph.index") || name.equals("type_subgraph.index");
//	}
//
//	protected final void closeCursor(Cursor cursor)
//	{
//		if (cursor != null)
//		{
//			try
//			{
//				cursor.close();
//			}
//			catch (Exception e)
//			{
//				// do nothing
//			}
//		}
//	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append('(');
		sb.append(getName());
		sb.append(')');

		return sb.toString();
	}

	@Override
	public HGIndexStats<KeyType, ValueType> stats()
	{
		// TODO Auto-generated method stub
		return null;
	}
}