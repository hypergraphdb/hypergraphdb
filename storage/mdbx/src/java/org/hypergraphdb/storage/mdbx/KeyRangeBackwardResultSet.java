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
import com.castortech.mdbxjni.OperationStatus;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

class KeyRangeBackwardResultSet<T> extends IndexResultSet<T>
{
	private DatabaseEntry initialKey = null;
	private ByteArrayConverter<T> converter;

	public KeyRangeBackwardResultSet(MdbxTxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
	{
		super(cursor, key, converter);
		initialKey = new DatabaseEntry();
		assignData(initialKey, key.getData());
	}

	@Override
	protected T advance()
	{
		checkCursor();
		try
		{
			OperationStatus status = cursor.cursor().get(CursorOp.PREV, key, data);
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
	protected T back()
	{
		if (HGUtils.eq(key.getData(), initialKey.getData()))
			return null;

		try
		{
			checkCursor();
			OperationStatus status = cursor.cursor().get(CursorOp.NEXT, key, data);
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
		return true;
	}
}