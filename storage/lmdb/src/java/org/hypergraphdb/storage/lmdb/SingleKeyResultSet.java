/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.lmdbjava.SeekOp;

/**
 * <p>
 * Implements a BerkeleyDB <code>Cursor</code> based result set that iterates
 * over all duplicates of a given key.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SingleKeyResultSet<BufferType, T> extends IndexResultSet<BufferType, T>
{
//	private boolean ordered = false;

	public SingleKeyResultSet(LMDBTxCursor<BufferType> cursor, 
							  BufferType key,
							  ByteArrayConverter<T> converter,
	    					  HGBufferProxyLMDB<BufferType> hgBufferProxy)
	{
		super(cursor, key, converter, hgBufferProxy);
//		try
//		{
//			ordered = cursor.cursor().getDatabase()
//					.getConfig(cursor.txn().getDbTransaction()).isDupSort();
//		}
//		catch (Throwable t)
//		{
//			throw new HGException(t);
//		}
	}

	protected T advance()
	{
		checkCursor();
		try
		{
			if (cursor.cursor().get(key, data, SeekOp.MDB_NEXT_DUP))			
			{
				byte [] value = this.hgBufferProxy.toBytes(data);
				return converter.fromByteArray(value, 0, value.length);
			}
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
			if (cursor.cursor().get(key, data, SeekOp.MDB_PREV_DUP))
			{
				byte [] value = this.hgBufferProxy.toBytes(data);
				return converter.fromByteArray(value, 0, value.length);
			}
			else
				return null;
		}
		catch (Throwable t)
		{
			closeNoException();
			throw new HGException(t);
		}
	}

	public boolean isOrdered()
	{
		return true;
	}
}
