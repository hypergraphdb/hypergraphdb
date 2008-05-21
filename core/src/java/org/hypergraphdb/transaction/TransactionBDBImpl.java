package org.hypergraphdb.transaction;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Transaction;

public class TransactionBDBImpl implements HGTransaction
{
	private Transaction t;
	private HashMap<String, Object> attributes = new HashMap<String, Object>();
	private Set<BDBTxCursor> bdbCursors = new HashSet<BDBTxCursor>();
	
	public static TransactionBDBImpl nullTransaction() { return new TransactionBDBImpl(null); }
	
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
			for (BDBTxCursor c : bdbCursors)
				c.close();
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
			for (BDBTxCursor c : bdbCursors)
				c.close();			
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