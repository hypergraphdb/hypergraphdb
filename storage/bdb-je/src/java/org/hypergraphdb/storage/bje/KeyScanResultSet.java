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

/**
 * 
 * <p>
 * Scans the key elements of an index. Similar to KeyRangeForwardResultSet, but instead of returning the data,
 * it returns the keys.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class KeyScanResultSet<T> extends IndexResultSet<T> {
	@Override
	protected T advance() {
		try {
			OperationStatus status = cursor.cursor().getNextNoDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(key.getData(), key.getOffset(), key.getSize());
			else
				return null;
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	@Override
	protected T back() {
		try {
			OperationStatus status = cursor.cursor().getPrevNoDup(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(key.getData(), key.getOffset(), key.getSize());
			else
				return null;
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered() {
		return true;
	}

	public KeyScanResultSet(BJETxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter) {
		this.converter = converter;
		this.cursor = cursor;
		this.key = new DatabaseEntry();
		
		if (keyIn != null) {
			assignData(key, keyIn.getData());
		}
		
		try {
			cursor.cursor().getCurrent(key, data, LockMode.DEFAULT);
			next = converter.fromByteArray(key.getData(), key.getOffset(), key.getSize());
			lookahead = 1;
		}
		catch (Throwable t) {
			throw new HGException(t);
		}
	}

	public GotoResult goTo(T value, boolean exactMatch) {
		byte[] B = converter.toByteArray(value);
		assignData(key, B);
		
		try {
			if (exactMatch) {
				if (cursor.cursor().getSearchKey(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					positionToCurrent(key.getData(), key.getOffset(), key.getSize());
					return GotoResult.found;
				}
				else
					return GotoResult.nothing;
			}
			else {
				if (cursor.cursor().getSearchKeyRange(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					positionToCurrent(key.getData(), key.getOffset(), key.getSize());
					return HGUtils.eq(B, key.getData()) ? GotoResult.found : GotoResult.close;
				}
				else
					return GotoResult.nothing;
			}
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}
}