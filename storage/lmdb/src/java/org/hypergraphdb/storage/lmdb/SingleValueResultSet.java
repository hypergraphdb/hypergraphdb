/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.OperationStatus;
import org.fusesource.lmdbjni.SecondaryCursor;
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

	public SingleValueResultSet(LmdbTxCursor cursor, DatabaseEntry keyIn,
			ByteArrayConverter<T> converter)
	{
		this.converter = converter;
		this.cursor = cursor;
		this.key = new DatabaseEntry();
		this.data = new DatabaseEntry();
		if (keyIn != null)
			assignData(key, keyIn.getData());
		try
		{
			((SecondaryCursor) cursor.cursor()).get(CursorOp.GET_CURRENT, key,
					pkey, data);
			next = converter.fromByteArray(pkey.getData(), 0, pkey.getData().length);
			lookahead = 1;
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}

	}

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
		catch (Throwable t)
		{
			closeNoException();
			throw new HGException(t);
		}
	}

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
		catch (Throwable t)
		{
			closeNoException();
			throw new HGException(t);
		}
	}

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
		catch (Throwable t)
		{
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered()
	{
		return false;
	}
}
