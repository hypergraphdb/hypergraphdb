package org.hypergraphdb.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.BDBTxLock;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.TransactionBDBImpl;

/**
 * 
 * <p>
 * An implementation of <code>ReadWriteLock</code> that will use the currently
 * active database transaction if there is one (through <code>BDBTxLock</code>) 
 * or a default <code>ReentrantReadWriteLock</code> if there is no current transaction.
 * The implementation is useful for runtime data structures that need protection
 * from concurrent access and that may or may not participate in a database
 * transaction. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGLock implements ReadWriteLock
{
	private ReentrantReadWriteLock defaultLock = null;
	private BDBTxLock txLock = null;
	
	ReadWriteLock getApplicableLock()
	{
		HGTransaction tx = txLock.getGraph().getTransactionManager().getContext().getCurrent(); 
		if (tx == null || ! (tx instanceof TransactionBDBImpl))
		{
			if (defaultLock == null)
				defaultLock = new ReentrantReadWriteLock();
			return defaultLock;
		}
		else
			return txLock;
	}
	
	public HGLock(HyperGraph graph, byte [] objectId)
	{
		txLock = new BDBTxLock(graph, objectId);
	}
	
	public Lock readLock()
	{
		return getApplicableLock().readLock();
	}

	public Lock writeLock()
	{
		return getApplicableLock().writeLock();
	}
}