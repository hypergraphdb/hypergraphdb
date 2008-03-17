package org.hypergraphdb.transaction;

import java.util.Stack;

import org.hypergraphdb.HGException;

class DefaultTransactionContext implements HGTransactionContext
{
	private Stack<HGTransaction> tstack = new Stack<HGTransaction>();
	private HGTransactionManager manager = null;
	
	DefaultTransactionContext(HGTransactionManager manager)
	{
		this.manager = manager;
	}
	
	/**
	 * <p>Return the currently active transaction or <code>null</code> if there is 
	 * no such transaction.</p>
	 */
	public HGTransaction getCurrent()
	{
		return tstack.isEmpty() ? null : tstack.peek();
	}
	
	public void beginTransaction()
	{
		if (tstack.isEmpty())
			tstack.push(manager.createTransaction(null));
		else
			tstack.push(manager.createTransaction(tstack.peek()));
	}
	
	public void endTransaction(boolean success) throws HGTransactionException
	{
		if (tstack.isEmpty())
			throw new HGException("Attempt to end a transaction for an empty transaction context.");
		HGTransaction top = tstack.pop();
		if (success)
			top.commit();
		else
			top.abort();
	}
	
	public void endAll(boolean success) throws HGTransactionException
	{
		if (success)
			while (!tstack.isEmpty()) tstack.pop().commit();
		else
			while (!tstack.isEmpty()) tstack.pop().abort();			
	}
}
