/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage.mdbx;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;

import com.castortech.mdbxjni.CursorOp;
import com.castortech.mdbxjni.DatabaseEntry;
import com.castortech.mdbxjni.OperationStatus;

/**
 * <p>
 * Implements a BerkeleyDB <code>Cursor</code> based result set that iterates
 * over all duplicates of a given key.
 * </p>
 *
 * @author Borislav Iordanov
 */
public class SingleKeyResultSet<T> extends IndexResultSet<T>
{
	private boolean ordered = false;

	public SingleKeyResultSet(MdbxTxCursor cursor, 
							  DatabaseEntry key,
							  ByteArrayConverter<T> converter)
	{
		super(cursor, key, converter);
		try
		{
			ordered = cursor.cursor().getDatabase()
					.getConfig(cursor.txn().getDbTransaction()).isDupSort();
		}
		catch (Exception e)
		{
			throw new HGException(e);
		}
	}

	@Override
	protected T advance()
	{
		if (!isOrdered()) // there is nothing else to return
			return null;

		checkCursor();
		try
		{
			OperationStatus status = cursor.cursor().get(CursorOp.NEXT_DUP, key,
					data);
			if (status == OperationStatus.SUCCESS)
			{
				return converter.fromByteArray(data.getData(), 0, data.getData().length);
			}
			else
				return null;
		}
		catch (Exception e)
		{
			closeNoException();
			throw new HGException(e);
		}
	}

	@Override
	protected T back()
	{
		checkCursor();
		try
		{
			OperationStatus status = cursor.cursor().get(CursorOp.PREV_DUP, key,
					data);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(data.getData(), 0, data.getData().length);
			else
				return null;
		}
		catch (Exception e)
		{
			closeNoException();
			throw new HGException(e);
		}
	}

	@Override
	public boolean isOrdered()
	{
		return ordered;
	}
}