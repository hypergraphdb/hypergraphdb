/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.CloseMe;

import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.LMDBException;

public final class LmdbTxCursor implements CloseMe
{
	private TransactionLmdbImpl tx;
	private Cursor cursor = null;
	private boolean open = true;
	
	public LmdbTxCursor(Cursor cursor, TransactionLmdbImpl tx)
	{
		this.cursor = cursor;
		this.tx = tx;
		open = cursor != null;
	}

	public Cursor cursor()
	{
		return cursor;
	}
	
  protected TransactionLmdbImpl txn()
	{
		return tx;
	}
	
	public boolean isOpen()
	{
		return open;
	}
	
	public void close()
	{
		if (!open)
			return;
		try 
		{
			cursor.close();
		}
		catch (LMDBException ex)
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
