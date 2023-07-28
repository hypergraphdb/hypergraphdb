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
import com.castortech.mdbxjni.SecondaryCursor;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A result set based on a BerkeleyDB secondary cursor. That is, when a
 * BerkeleyDB has a secondary DB, it is possible to use the keys of the
 * secondary DB to get primary key and data of the primary DB. This result set
 * returns primary keys based on secondary keys and it ignores the data (which
 * usually will simply be the same thing as the secondary key).
 * </p>
 *
 * @author Borislav Iordanov
 */
public class SingleValueResultSet<T> extends IndexResultSet<T>
{
	private DatabaseEntry pkey = new DatabaseEntry();

	public SingleValueResultSet(MdbxTxCursor cursor, 
								DatabaseEntry keyIn,
								ByteArrayConverter<T> converter)
	{
		MDBXUtils.checkArgNotNull(cursor, "cursor");

		//
		// The following is bit hacky because we want to avoid some of the
		// default behavior
		// of the super constructor, which is incorrect when the "values" we are
		// interested in
		// are the DB's primary keys. So we duplicate its behavior and override
		// instantiation
		// of the current value.
		this.converter = converter;
		this.cursor = cursor;
		key = new DatabaseEntry();
		data = new DatabaseEntry();
		if (keyIn != null)
			assignData(key, keyIn.getData());

		try
		{
			((SecondaryCursor) cursor.cursor()).get(CursorOp.GET_CURRENT, key,
					pkey, data);
			next = converter.fromByteArray(pkey.getData(), 0, key.getData().length);
			lookahead = 1;
		}
		catch (Exception e)
		{
			throw new HGException(e);
		}
	}

	@Override
	protected T advance()
	{
		checkCursor();
		try
		{
			pkey = new DatabaseEntry();
			OperationStatus status = ((SecondaryCursor) cursor.cursor())
					.get(CursorOp.NEXT_DUP, key, pkey, data);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(pkey.getData(), 0, pkey.getData().length);
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
			pkey = new DatabaseEntry();
			OperationStatus status = ((SecondaryCursor) cursor.cursor())
					.get(CursorOp.PREV_DUP, key, pkey, data);
			if (status == OperationStatus.SUCCESS)
				return converter.fromByteArray(pkey.getData(), 0, pkey.getData().length);
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
	public GotoResult goTo(T value, boolean exactMatch)
	{
		byte[] B = converter.toByteArray(value);
		assignData(pkey, B);
		try
		{
			if (exactMatch)
			{
				OperationStatus status = ((SecondaryCursor) cursor.cursor())
						.get(CursorOp.GET_BOTH, key, pkey, data);
				if (status == OperationStatus.SUCCESS)
				{
					positionToCurrent(pkey.getData());
					return GotoResult.found;
				}
				else
					return GotoResult.nothing;
			}
			else
			{
				OperationStatus status = ((SecondaryCursor) cursor.cursor())
						.get(CursorOp.GET_BOTH_RANGE, key, pkey, data);
				if (status == OperationStatus.SUCCESS)
				{
					positionToCurrent(pkey.getData());
					return HGUtils.eq(B, pkey.getData()) ? GotoResult.found
							: GotoResult.close;
				}
				else
					return GotoResult.nothing;
			}
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
		return false;
	}
}