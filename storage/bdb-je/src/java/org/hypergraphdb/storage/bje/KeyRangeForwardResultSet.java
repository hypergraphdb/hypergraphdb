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
import org.hypergraphdb.util.HGUtils;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

class KeyRangeForwardResultSet<T> extends IndexResultSet<T> {
	private DatabaseEntry initialKey = null;

	protected T advance() {
		try {
			OperationStatus status = cursor.cursor().getNext(key, data, LockMode.DEFAULT);
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

	protected T back() {
		try {
			// The range starts at the values of initialKey: moving back from one of them must not
			// leave that key (it is the smallest key of the range).
			boolean atInitialKey = HGUtils.eq(key.getData(), initialKey.getData());
			OperationStatus status = cursor.cursor().getPrev(key, data, LockMode.DEFAULT);
			if (status != OperationStatus.SUCCESS)
				return null;
			if (atInitialKey && !HGUtils.eq(key.getData(), initialKey.getData())) {
				cursor.cursor().getNext(key, data, LockMode.DEFAULT);
				return null;
			}
			return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public KeyRangeForwardResultSet(BJETxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter) {
		super(cursor, key, converter);
		initialKey = new DatabaseEntry();
		assignData(initialKey, key.getData());
	}

	/** Position on the first value of the range's first key. */
	public void goBeforeFirst() {
		try {
			key = new DatabaseEntry(initialKey.getData().clone());
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

	/** The range extends to the end of the database: position on its last record. */
	public void goAfterLast() {
		try {
			if (cursor.cursor().getLast(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				positionedAfterLast(converter.fromByteArray(data.getData(), data.getOffset(), data.getSize()));
			else
				positionedNowhere();
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered() {
		return true;
	}
}