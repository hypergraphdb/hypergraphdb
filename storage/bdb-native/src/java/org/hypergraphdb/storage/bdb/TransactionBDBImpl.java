/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.bdb;


import java.util.HashSet;
import java.util.Set;

import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;

public class TransactionBDBImpl implements HGStorageTransaction
{
	private Environment env;
	private Transaction t;
	private Set<BDBTxCursor> bdbCursors = new HashSet<BDBTxCursor>();
	private boolean aborting = false;
	
	public static final TransactionBDBImpl nullTransaction() 
	{ 
	    return new TransactionBDBImpl(null, null); 
	}
	
	public TransactionBDBImpl(Transaction t, Environment env)
	{
		this.t = t;
		this.env = env;
	}
		
	public Environment getBDBEnvironment()
	{
		return env;
	}
	
	public Transaction getBDBTransaction()
	{
		return t;
	}
	
	public void commit() throws HGTransactionException
	{
		try
		{
			// Since 'close' removes from the set, we need to clone first to
			// avoid a ConcurrentModificationException
			Set<BDBTxCursor> tmp = new HashSet<BDBTxCursor>();
			tmp.addAll(bdbCursors);
			for (BDBTxCursor c : tmp)
				try { c.close(); }
				catch (Throwable t) { System.err.println(t); }
			if (t != null)
				t.commit();
		}
		catch (DatabaseException ex)
		{
			throw new HGTransactionException("Failed to commit transaction", ex);
		}
	}
	
	public void abort() throws HGTransactionException
	{
		try
		{
			aborting = true;
			// Since 'close' removes from the set, we need to clone first to
			// avoid a ConcurrentModificationException
			Set<BDBTxCursor> tmp = new HashSet<BDBTxCursor>();
			tmp.addAll(bdbCursors);
			for (BDBTxCursor c : tmp)
				try { c.close(); }
				catch (Throwable t) { System.err.println(t); }
			if (t != null)
				t.abort();
		}
		catch (DatabaseException ex)
		{
			throw new HGTransactionException("Failed to abort transaction", ex);
		}		
	}

	public BDBTxCursor attachCursor(Cursor cursor)
	{
		if (t == null)
			return new BDBTxCursor(cursor, null);
		BDBTxCursor c = new BDBTxCursor(cursor, this);
		bdbCursors.add(c);
		return c;
	}
	
	void removeCursor(BDBTxCursor c)
	{
		if (!aborting)
			bdbCursors.remove(c);
	}	
}