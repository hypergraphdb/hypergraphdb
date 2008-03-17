package org.hypergraphdb.transaction;

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
public interface HGTransactionContext
{
	HGTransaction getCurrent();
	void beginTransaction();
	void endTransaction(boolean success) throws HGTransactionException;
	void endAll(boolean success) throws HGTransactionException;
}