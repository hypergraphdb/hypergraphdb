/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.event.HGTransactionEndEvent;


import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;

public class TransactionBDBImpl implements HGTransaction
{
    private HGTransactionContext context;
	private Environment env;
	private Transaction t;
	private HashMap<String, Object> attributes = new HashMap<String, Object>();
	private Set<BDBTxCursor> bdbCursors = new HashSet<BDBTxCursor>();
	private boolean aborting = false;
	
	public static final TransactionBDBImpl nullTransaction() { return new TransactionBDBImpl(null, null, null); }
	
	public TransactionBDBImpl(HGTransactionContext context, Transaction t, Environment env)
	{
		this.t = t;
		this.env = env;
		this.context = context;
	}
	
	public HGTransactionContext getContext()
	{
	    return context;
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
			for (BDBTxCursor c : bdbCursors)
				c.close();
			if (t != null)
				t.commit();
			HyperGraph graph = context.getManager().getHyperGraph();
			graph.getEventManager().dispatch(graph, new HGTransactionEndEvent(this, true));
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
			for (BDBTxCursor c : bdbCursors)
				try { c.close(); }
				catch (Throwable t) { System.err.println(t); }
			if (t != null)
				t.abort();
            HyperGraph graph = context.getManager().getHyperGraph();
            graph.getEventManager().dispatch(graph, new HGTransactionEndEvent(this, false));			
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
	
	public Object getAttribute(String name) 
	{
		return attributes.get(name);
	}

	public Iterator<String> getAttributeNames() 
	{
		return attributes.keySet().iterator();
	}

	public void removeAttribute(String name) 
	{
		attributes.remove(name);
	}

	public void setAttribute(String name, Object value) 
	{
		attributes.put(name, value);
	}
}