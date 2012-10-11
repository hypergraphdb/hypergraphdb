/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;

import java.util.Comparator;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.transaction.HGTransactionManager;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

@SuppressWarnings("unchecked")
public class DefaultBiIndexImpl<KeyType, ValueType> extends DefaultIndexImpl<KeyType, ValueType> implements
		HGBidirectionalIndex<KeyType, ValueType> {
	private static final String SECONDARY_DB_NAME_PREFIX = DB_NAME_PREFIX + "_secondary";

	private DatabaseEntry dummy = new DatabaseEntry();
	private SecondaryDatabase secondaryDb = null;

	public DefaultBiIndexImpl(String indexName, BJEStorageImplementation storage,
			HGTransactionManager transactionManager, ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter, Comparator<?> comparator) {
		super(indexName, storage, transactionManager, keyConverter, valueConverter, comparator);
	}

	public void open() {
		sort_duplicates = false;
		super.open();

		try {
			SecondaryConfig dbConfig = new SecondaryConfig();
			dbConfig.setAllowCreate(true);
			
			if (storage.getBerkleyEnvironment().getConfig().getTransactional()) {
				dbConfig.setTransactional(true);
			}
			
			dbConfig.setKeyCreator(PlainSecondaryKeyCreator.getInstance());
			dbConfig.setSortedDuplicates(true);
			secondaryDb = storage.getBerkleyEnvironment().openSecondaryDatabase(null, SECONDARY_DB_NAME_PREFIX + name, 
							db, dbConfig);
		}
		catch (Throwable t) {
			throw new HGException("While attempting to open index ;" + name + "': " + t.toString(), t);
		}
	}

	public void close() {
		HGException exception = null;

		try {
			super.close();
		}
		catch (HGException ex) {
			exception = ex;
		}

		if (secondaryDb == null)
			return;

		// Attempt to close secondary database even if there was an exception
		// during the close of the primary.
		try {
			secondaryDb.close();
		}
		catch (Throwable t) {
			if (exception == null)
				exception = new HGException(t);
		}
		finally {
			secondaryDb = null;
		}

		if (exception != null)
			throw exception;
	}

	public boolean isOpen() {
		return super.isOpen() && secondaryDb != null;
	}

	public void addEntry(KeyType key, ValueType value) {
		checkOpen();
		DatabaseEntry dbkey = new DatabaseEntry(keyConverter.toByteArray(key));
		DatabaseEntry dbvalue = new DatabaseEntry(valueConverter.toByteArray(value));
		
		try {
			OperationStatus result = db.put(txn().getBJETransaction(), dbkey, dbvalue);
			if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
				throw new Exception("OperationStatus: " + result);
		}
		catch (Exception ex) {
			throw new HGException("Failed to add entry to index '" + name + "': " + ex.toString(), ex);
		}
	}

	public HGRandomAccessResult<KeyType> findByValue(ValueType value) {
		if (!isOpen())
			throw new HGException("Attempting to lookup index '" + name + "' while it is closed.");
		/*
		 * if (value == null) throw new HGException("Attempting to lookup index '" + name + "' with a null key.");
		 */
		DatabaseEntry keyEntry = new DatabaseEntry(valueConverter.toByteArray(value));
		DatabaseEntry valueEntry = new DatabaseEntry();
		HGRandomAccessResult<KeyType> result = null;
		SecondaryCursor cursor = null;
		
		try {
			TransactionBJEImpl tx = txn();
			cursor = secondaryDb.openCursor(tx.getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, dummy, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS /* && cursor.count() > 0 */)
				result = new SingleValueResultSet<KeyType>(tx.attachCursor(cursor), keyEntry, keyConverter);
			else {
				try {
					cursor.close();
				}
				catch (Throwable t) {
				}
				result = (HGRandomAccessResult<KeyType>)HGSearchResult.EMPTY;
			}
		}
		catch (Exception ex) {
			if (cursor != null)
				try {
					cursor.close();
				}
				catch (Throwable t) {
				}
			throw new HGException("Failed to lookup index '" + name + "': " + ex.toString(), ex);
		}
		return result;
	}

	public KeyType findFirstByValue(ValueType value) {
		if (!isOpen())
			throw new HGException("Attempting to lookup by value index '" + name + "' while it is closed.");
		
		/*
		 * if (value == null) throw new HGException("Attempting to lookup by value index '" + name +
		 * "' with a null value.");
		 */
		DatabaseEntry keyEntry = new DatabaseEntry(valueConverter.toByteArray(value));
		DatabaseEntry valueEntry = new DatabaseEntry();
		KeyType result = null;
		SecondaryCursor cursor = null;
		
		try {
			cursor = secondaryDb.openCursor(txn().getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, dummy, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				result = keyConverter.fromByteArray(valueEntry.getData(), valueEntry.getOffset(), valueEntry.getSize());
		}
		catch (Exception ex) {
			throw new HGException("Failed to lookup index '" + name + "': " + ex.toString(), ex);
		}
		finally {
			if (cursor != null) {
				try {
					cursor.close();
				}
				catch (Throwable t) {
				}
			}
		}
		return result;
	}

	public long countKeys(ValueType value) {
		DatabaseEntry keyEntry = new DatabaseEntry(valueConverter.toByteArray(value));
		DatabaseEntry valueEntry = new DatabaseEntry();
		SecondaryCursor cursor = null;
		
		try {
			cursor = secondaryDb.openCursor(txn().getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, dummy, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return cursor.count();
			else
				return 0;
		}
		catch (DatabaseException ex) {
			throw new HGException(ex);
		}
		finally {
			if (cursor != null) {
				try {
					cursor.close();
				}
				catch (Throwable t) {
				}
			}
		}
	}
}