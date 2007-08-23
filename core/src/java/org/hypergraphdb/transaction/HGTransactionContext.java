package org.hypergraphdb.transaction;

import org.hypergraphdb.HGException;
import java.util.Stack;

/**
 * 
 * <p>
 * A transaction context maintains a stack of nested transactions. The transaction
 * on top of the stack is the currently active transaction. A <code>beginTransaction</code>
 * will create and push a new transaction on the stack and an <code>endTransaction</code>
 * will remove the top of the transaction stack and commit or abort.
 * </p>
 * 
 * <p>
 * Transaction contexts are useful when implementing a server for HyperGraph. A context can
 * be bound to a client and then get dynamically attached/detach to worker threads using the
 * <code>HGTransactionManager.threadAttach</code> and <code>HGTransactionManager.threadDetach</code>
 * methods.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGTransactionContext
{
	private Stack<HGTransaction> tstack = new Stack<HGTransaction>();
	private HGTransactionManager manager = null;
	
	HGTransactionContext(HGTransactionManager manager)
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