/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * <p>
 * Implements a BerkeleyDB <code>Cursor</code> based result set that iterates over all duplicates of a given
 * key.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SingleKeyResultSet<T> extends IndexResultSet<T> {
	private boolean ordered = false;
	// The key whose values this result set iterates.
	private final byte[] keyBytes;

	public SingleKeyResultSet(BJETxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter) {
		super(cursor, key, converter);
		this.keyBytes = java.util.Arrays.copyOfRange(this.key.getData(), this.key.getOffset(), this.key.getOffset() + this.key.getSize());
		try {
			ordered = cursor.cursor().getDatabase().getConfig().getSortedDuplicates();
		}
		catch (Throwable t) {
			throw new HGException(t);
		}
	}

	/** Position on the first value of this result set's key. */
	public void goBeforeFirst() {
		try {
			key = new DatabaseEntry(keyBytes.clone());
			if (cursor.cursor().getSearchKey(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				positionedBeforeFirst(converter.fromByteArray(data.getData(), data.getOffset(), data.getSize()));
			else
				positionedNowhere();
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	/**
	 * Position on the last value of this result set's key: move to the next key and back one record,
	 * or to the last record of the database if this key is the last one.
	 */
	public void goAfterLast() {
		try {
			key = new DatabaseEntry(keyBytes.clone());
			OperationStatus status = cursor.cursor().getSearchKey(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				if (cursor.cursor().getNextNoDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
					status = cursor.cursor().getPrev(key, data, LockMode.DEFAULT);
				else
					status = cursor.cursor().getLast(key, data, LockMode.DEFAULT);
			}
			if (status == OperationStatus.SUCCESS)
				positionedAfterLast(converter.fromByteArray(data.getData(), data.getOffset(), data.getSize()));
			else
				positionedNowhere();
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	/**
	 * Moves the cursor to the next duplicate record, if exists, else null
	 * @return
	 */
	protected T advance() {
		try {
			OperationStatus status = cursor.cursor().getNextDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	/**
	 * Moves the cursor to the previous duplicate record, if exists, else null
	 * @return
	 */
	protected T back() {
		try {
			OperationStatus status = cursor.cursor().getPrevDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
			else
				return null;
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered() {
		return ordered;
	}
}