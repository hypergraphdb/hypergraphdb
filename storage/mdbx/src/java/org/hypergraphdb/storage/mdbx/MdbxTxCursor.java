/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage.mdbx;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.CloseMe;

import com.castortech.mdbxjni.Cursor;
import com.castortech.mdbxjni.MDBXException;

public final class MdbxTxCursor implements CloseMe
{
	private TransactionMdbxImpl tx;
	private Cursor cursor = null;
	private boolean open = true;

	public MdbxTxCursor(Cursor cursor, TransactionMdbxImpl tx)
	{
		this.cursor = cursor;
		this.tx = tx;
		open = cursor != null;
	}

	public Cursor cursor()
	{
		return cursor;
	}

	protected TransactionMdbxImpl txn()
	{
		return tx;
	}

	public boolean isOpen()
	{
		return open;
	}

	@Override
	public void close()
	{
		if (!open)
			return;

		try
		{
			cursor.close();
		}
		catch (MDBXException ex)
		{
			throw new HGException(ex);
		}
		finally
		{
			open = false;
			cursor = null;
			if (tx != null)
				tx.removeCursor(this);
		}
	}
}