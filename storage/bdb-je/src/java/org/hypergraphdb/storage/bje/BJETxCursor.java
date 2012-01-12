/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.CloseMe;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;

public final class BJETxCursor implements CloseMe {
	private TransactionBJEImpl tx;
	private Cursor cursor = null;
	private boolean open = true;

	public BJETxCursor(Cursor cursor, TransactionBJEImpl tx) {
		this.cursor = cursor;
		this.tx = tx;
		open = cursor != null;
	}

	public Cursor cursor() {
		return cursor;
	}

	public boolean isOpen() {
		return open;
	}

	public void close() {
		if (!open)
			return;
		try {
			cursor.close();
		}
		catch (DatabaseException ex) {
			throw new HGException(ex);
		}
		finally {
			open = false;
			cursor = null;
			if (tx != null) {
				tx.removeCursor(this);
			}
		}
	}
}
