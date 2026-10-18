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
import com.sleepycat.je.SecondaryCursor;

/**
 * <p>
 * A result set based on a BerkeleyDB secondary cursor. That is, when a BerkeleyDB has a secondary DB, it is
 * possible to use the keys of the secondary DB to get primary key and data of the primary DB. This result set
 * returns primary keys based on secondary keys and it ignores the data (which usually will simply be the same
 * thing as the secondary key).
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SingleValueResultSet<T> extends IndexResultSet<T> {
	private DatabaseEntry pkey = new DatabaseEntry();
	// The (secondary) key whose primary keys this result set iterates.
	private byte[] keyBytes;

	public SingleValueResultSet(BJETxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter) {
		//
		// The following is bit hacky because we want to avoid some of the default behavior
		// of the super constructor, which is incorrect when the "values" we are interested in 
		// are the DB's primary keys. So we duplicate its bebavior and override instantiation
		// of the current value.
		this.converter = converter;
		this.cursor = cursor;
		this.key = new DatabaseEntry();
		if (keyIn != null) {
			assignData(key, keyIn.getData());
		}
		
		try {
			checkFound(((SecondaryCursor)cursor.cursor()).getCurrent(key, pkey, data, LockMode.DEFAULT), "getCurrent");
			keyBytes = java.util.Arrays.copyOfRange(key.getData(), key.getOffset(), key.getOffset() + key.getSize());
			next = converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
			lookahead = 1;
		}
		catch (Throwable t) {
			throw new HGException(t);
		}

	}

	protected T advance() {
		try {
			OperationStatus status =
					((SecondaryCursor)cursor.cursor()).getNextDup(key, pkey, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
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
			OperationStatus status =
					((SecondaryCursor)cursor.cursor()).getPrevDup(key, pkey, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
			else
				return null;
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public GotoResult goTo(T value, boolean exactMatch) {
		byte[] B = converter.toByteArray(value);
		assignData(pkey, B);
		try {
			if (exactMatch) {
				if (((SecondaryCursor)cursor.cursor()).getSearchBoth(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					positionToCurrent(pkey.getData(), pkey.getOffset(), pkey.getSize());
					return GotoResult.found;
				}
				else
					return GotoResult.nothing;
			}
			else {
				if (((SecondaryCursor)cursor.cursor()).getSearchBothRange(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					positionToCurrent(pkey.getData(), pkey.getOffset(), pkey.getSize());
					return HGUtils.eq(B, pkey.getData()) ? GotoResult.found : GotoResult.close;
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

	/** Position on the first primary key for this result set's (secondary) key. */
	public void goBeforeFirst() {
		try {
			key = new DatabaseEntry(keyBytes.clone());
			if (((SecondaryCursor)cursor.cursor()).getSearchKey(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				positionedBeforeFirst(converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize()));
			else
				positionedNowhere();
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	/** Position on the last primary key for this result set's (secondary) key. */
	public void goAfterLast() {
		try {
			SecondaryCursor c = (SecondaryCursor)cursor.cursor();
			key = new DatabaseEntry(keyBytes.clone());
			OperationStatus status = c.getSearchKey(key, pkey, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				if (c.getNextNoDup(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
					status = c.getPrev(key, pkey, data, LockMode.DEFAULT);
				else
					status = c.getLast(key, pkey, data, LockMode.DEFAULT);
			}
			if (status == OperationStatus.SUCCESS)
				positionedAfterLast(converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize()));
			else
				positionedNowhere();
		}
		catch (Throwable t) {
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered() {
		return false;
	}
}