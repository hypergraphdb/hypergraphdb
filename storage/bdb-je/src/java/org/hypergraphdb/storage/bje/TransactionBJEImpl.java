/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;

import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

public class TransactionBJEImpl implements HGStorageTransaction
{
	private Environment env;
	private Transaction t;
	private Set<BJETxCursor> bdbCursors = new HashSet<BJETxCursor>();
	private volatile boolean aborting = false;
	private volatile boolean attached;
	private volatile boolean detached;
	private volatile boolean committing = false;
//	public static volatile boolean traceme = false;
	
	public static final TransactionBJEImpl nullTransaction()
	{
		return new TransactionBJEImpl(null, null);
	}

	public TransactionBJEImpl(Transaction t, Environment env)
	{
		this.t = t;
		this.env = env;
//		if (t != null && traceme)
//			System.out.println("Created BJE tx " + t.getId());
	}

	public Environment getBJEEnvironment()
	{
		return env;
	}

	public Transaction getBJETransaction()
	{
		return t;
	}

	public void commit() throws HGTransactionException
	{
//		if (t != null && traceme)
//			System.err.println("Committing tx " + t.getId());
		try
		{
			attached = false;
			detached = false;
			committing = true;
			Set<BJETxCursor> S = new HashSet<BJETxCursor>(bdbCursors);
			committing = false;
			//for (BJETxCursor c : bdbCursors) S.add(c);
			for (BJETxCursor c : S)
				c.close();
			if (t != null)
				t.commit();
		}
		catch (ConcurrentModificationException ex)
		{
			if (attached)
				throw new ConcurrentModificationException("Somebody attached cursor");
			else if (detached)
				throw new ConcurrentModificationException("Sombody detached cursor");
			else
				throw new ConcurrentModificationException("Nobody touched cursor");
		}
		catch (DatabaseException ex)
		{
			throw new HGTransactionException("Failed to commit transaction",
					ex);
		}
	}

	public void abort() throws HGTransactionException
	{
//		if (t != null && traceme)
//			System.err.println("Aborting tx " + t.getId());
		try
		{
//			aborting = true;
			Set<BJETxCursor> S = new HashSet<BJETxCursor>(bdbCursors);
			for (BJETxCursor c : S)
			{
				try
				{
					c.close();
				}
				catch (Throwable t)
				{
					System.err.println(t);
				}
			}
			if (t != null)
			{
				t.abort();
			}
		}
		catch (DatabaseException ex)
		{
			throw new HGTransactionException("Failed to abort transaction", ex);
		}
	}

	public BJETxCursor attachCursor(Cursor cursor)
	{
		attached = true;
//		if (t != null && traceme)
//			System.err.println("Adding cursor to tx " + t.getId());
		if (t == null)
		{
			return new BJETxCursor(cursor, null);
		}
		BJETxCursor c = new BJETxCursor(cursor, this);
		bdbCursors.add(c);
		return c;
	}

	void removeCursor(BJETxCursor c)
	{
		if (committing)
			System.out.println("WTF - I am committing already!");
		detached = true;
//		if (t != null && traceme)		
//			System.err.println("Removing cursor from tx " + t.getId());
		if (!aborting)
		{
			bdbCursors.remove(c);
		}
	}
}