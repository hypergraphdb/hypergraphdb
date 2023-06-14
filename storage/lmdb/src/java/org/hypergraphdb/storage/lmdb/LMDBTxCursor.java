/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

//import org.hypergraphdb.HGException;
import org.hypergraphdb.util.CloseMe;
import org.lmdbjava.Cursor;


public final class LMDBTxCursor<BufferType> implements CloseMe
{
	private StorageTransactionLMDB<BufferType> tx;
	private Cursor<BufferType> cursor = null;
	private boolean open = true;
	
	public LMDBTxCursor(Cursor<BufferType> cursor, StorageTransactionLMDB<BufferType> tx)
	{
		this.cursor = cursor;
		this.tx = tx;
		open = cursor != null;
	}

	public Cursor<BufferType> cursor()
	{
		return cursor;
	}
	
  protected StorageTransactionLMDB<BufferType> txn()
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
//		catch (Exception ex)
//		{
//			throw new HGException(ex);
//		}
		finally
		{
			open = false;
			cursor = null;
//			if (tx != null)
//				tx.removeCursor(this);
		}		
	}
}
