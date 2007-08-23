package org.hypergraphdb.transaction;

import java.util.HashMap;
import java.util.Iterator;

import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Transaction;

public class TransactionBDBImpl implements HGTransaction
{
	private Transaction t;
	private HashMap<String, Object> attributes = new HashMap<String, Object>();
	
	public TransactionBDBImpl(Transaction t)
	{
		this.t = t;
	}
	
	public Transaction getBDBTransaction()
	{
		return t;
	}
	
	public void commit() throws HGTransactionException
	{
		try
		{
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
			if (t != null)
				t.abort();
		}
		catch (DatabaseException ex)
		{
			throw new HGTransactionException("Failed to abort transaction", ex);
		}		
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