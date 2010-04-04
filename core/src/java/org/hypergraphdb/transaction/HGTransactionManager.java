/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HyperGraph;

import com.sleepycat.db.DeadlockException;

/**
 * 
 * <p>
 * The <code>HGTransactionManager</code> handles transactional activity for a single
 * HyperGraph instance. You can obtain the transaction manager for a HyperGraph by
 * calling its <code>getTransactionManager</code> method.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGTransactionManager
{
    private HyperGraph graph;    
	private HGTransactionFactory factory;	
	private ThreadLocal<HGTransactionContext> tcontext =  new ThreadLocal<HGTransactionContext>();
	private boolean enabled = true;
	
    volatile ActiveTransactionsRecord mostRecentRecord = new ActiveTransactionsRecord(0, null);
    final ReentrantLock COMMIT_LOCK = new ReentrantLock(true);
        
	TxMonitor txMonitor = null;
	
	/** 
	 * <p>Return <code>true</code> if the transaction are enabled and <code>false</code>
	 * otherwise.</p>
	 */
	public boolean isEnabled()
	{
		return enabled;
	}
	
	/**
	 * <p>Enable or disable transaction. Note that all current transactions will
	 * be silently aborted so make sure any pending transactions are completed before
	 * calling this method.</p>
	 * 
	 * @param enabled <code>true</code> if transaction must be henceforth enabled and
	 * <code>false</code> otherwise.
	 */
	public void setEnabled(boolean enabled)
	{
		this.enabled = enabled;
	}
	
	/**
	 * <p>Enable transactions - equivalent to <code>setEnabled(true)</code>.</p>  
	 */
	public void enable() { setEnabled(true); }
	
	/**
	 * <p>Disable transactions - equivalent to <code>setEnabled(false)</code></p>
	 */
	public void disable() { setEnabled(false); }
	
	/**
	 * <p>Return the <code>HGTransactionContext</code> instance associated with the current
	 * thread.</p>
	 */
    public synchronized HGTransactionContext getContext()
    {
    	HGTransactionContext ctx = tcontext.get();
    	if (ctx == null)
    	{
    		ctx = new DefaultTransactionContext(this);
    		tcontext.set(ctx);
    	}
    	return ctx;
    }
    
    /**
     * <p>
     * Construct a new transaction manager with the given transaction factory. This
     * method is normally called only internally. To obtain the transactio manager
     * bound to a HyperGraph, use <code>HyperGraph.getTransactionManager</code>.
     * </p>
     * 
     * @param factory The <code>HGTransactionFactory</code> responsible for
     * fabricating new transactions.
     */
	public HGTransactionManager(HGTransactionFactory factory)
	{
		this.factory = factory;
	}

    /**
     * <p>
     * Set the {@link HyperGraph} instance associated with this
     * <code>HGTransactionManager</code>. 
     * <strong>Do not call - used internally during initialization.
     * </p>
     */
    public void setHyperGraph(HyperGraph graph)
    {
        this.graph = graph;         
    }
    
    /**
     * <p>Return the {@link HyperGraph} instance associated with this
     * <code>HGTransactionManager</code>.
     * </p> 
     */
    public HyperGraph getHyperGraph()
    {
        return graph;
    }
    
	/**
	 * <p>
	 * Attach the given transaction context to the current thread. This
	 * method is normally called in a server environment. By default, 
	 * a transaction context will be created and bound to a thread
	 * if need be, every time a new transaction is requested. So when 
	 * HyperGraph is embedded in a client application, there is no
	 * need to explicitly attach/detach contexts to threads. However, in 
	 * an environment using thread pooling such as is common in servers where
	 * a single transaction can span multiple requests, use the 
	 * <code>threadAttach</code> and <code>threadDetach</code> methods
	 * to switch transactions contexts bound to clients.  
	 * </p>
	 * 
	 * @param tContext
	 */
	public void threadAttach(HGTransactionContext tContext)
	{		
		tcontext.set(tContext);
	}

	/**
	 * <p>
	 * Detach the transaction context bound to the current thread. This
	 * method is normally called in a server environment. By default, 
	 * a transaction context will be created and bound to a thread
	 * if need be, every time a new transaction is requested. So when 
	 * HyperGraph is embedded in a client application, there is no
	 * need to explicitly attach/detach contexts to threads. 
	 * </p>
	 * 
	 * <p>
	 * <strong>IMPORTANT NOTE:</strong> when managing transaction
	 * contexts explicitly, you are responsible for closing
	 * all pending transactions in the context before disposing of it.
	 * This is done by invoking the <code>HGTransactionContext.endAll</code>
	 * method. 
	 * </p>
	 * 
	 * @param tContext
	 */
	public void threadDetach()
	{
		tcontext.set(null);
	}
	
	/**
	 * <p>
	 * Create and return a child transaction of the given parent transaction.
	 * </p>
	 * 
	 * @param The parent <code>HGTransaction</code> - if null, a top-level 
	 * transaction object is returned.
	 * @return The newly created transaction.
	 */
	public HGTransaction createTransaction(HGTransaction parent)
	{		 
		if (enabled)
		{
		    ActiveTransactionsRecord activeRecord = mostRecentRecord.getRecordForNewTransaction();
			HGTransaction result = new HGTransaction(getContext(),
			                                         parent,
			                                         activeRecord,
			                                         factory.createTransaction(getContext(), parent));
			if (txMonitor != null)
				txMonitor.transactionCreated(result);
			return result;
		}
		else
			return new HGTransaction(getContext(), parent, null, new VanillaTransaction());
	}
	
	/**
	 * <p>
	 * Begin a new transaction in the current transaction context. If there's
	 * no transaction context bound to the active thread, one will be created.
	 * </p>
	 */
	public void beginTransaction()
	{
		getContext().beginTransaction();
	}
	
	/**
	 * <p>
	 * Terminate the currently active transaction. The transaction will
	 * be aborted or committed based on the <code>success</code> flag 
	 * (abort when false and commit when true).
	 * </p>
	 * 
	 * <p>
	 * You are graced with a <code>HGException</code> if there's no currently
	 * active transaction.
	 * </p>
	 * 
	 * @param success
	 */
	public void endTransaction(boolean success) throws HGTransactionException
	{
		HGTransactionContext ctx = tcontext.get();
		if (ctx == null)
			throw new HGException("Attempt to end a transaction with no transaction context currently active.");
		else
			ctx.endTransaction(success);
	}
	
	/**
	 * <p>
	 * Commit the current transaction by calling <code>endTransaction(true)</code>. 
	 * Wrap the possible <code>HGTransactionException</code> in a <code>HGException</code>.
	 * </p>
	 */
	public void commit()
	{
		try
		{
			endTransaction(true);
		}
		catch (HGTransactionException ex)
		{
			throw new HGException(ex);
		}
	}
	
	/**
	 * <p>
	 * Abort the current transaction by calling <code>endTransaction(false)</code>. 
	 * Wrap the possible <code>HGTransactionException</code> in a <code>HGException</code>.
	 * </p>
	 */
	public void abort()
	{
		try
		{
			endTransaction(false);
		}
		catch (HGTransactionException ex)
		{
			throw new HGException(ex);
		}
	}

	/**
	 * <p>
	 * Perform a unit of work encapsulated as a transaction and return the result. This method
	 * will reuse the currently active transaction if there is one or create a new transaction
	 * otherwise.
	 * </p>
	 * 
	 * @param <V> The type of the return value.
	 * @param transaction The transaction process encapsulated as a <code>Callable</code> instance.
	 * @return The result of <code>transaction.call()</code>.
	 * @throws The method will (re)throw any exception that does not result from a deadlock.
	 */
	public <V> V ensureTransaction(Callable<V> transaction)
	{
		if (getContext().getCurrent() != null)
			try 
			{
				return transaction.call();
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
		else
			return transact(transaction);
	}
	
	private void handleTxException(Throwable t)
	{
        // If there is a DeadlockException at the root of this, we have to simply abort
        // the transaction and try again.               
        boolean retry = false;
        for (Throwable cause = t; cause != null; cause = cause.getCause())                   
            if (cause instanceof DeadlockException || 
                cause instanceof TransactionConflictException)
            {
                retry = true;
                break;
            }
        
        if (!retry)
        {
            if (t instanceof RuntimeException)
                throw (RuntimeException)t;
            else
                throw new HGException(t);
        }       	    
	}
	
	/**
	 * <p>
	 * Perform a unit of work encapsulated as a transaction and return the result. This method
	 * explicitly allows deadlock to occur and it will reattempt the transaction in such a 
	 * case indefinitely. In order for the transaction to eventually complete, the underlying
	 * transactional system must be configured to be fair or to prioritize transaction randomly
	 * (which is the default behavior).
	 * </p>
	 * 
	 * @param <V> The type of the return value.
	 * @param transaction The transaction process encapsulated as a <code>Callable</code> instance.
	 * @return The result of <code>transaction.call()</code>.
	 * @throws The method will (re)throw any exception that does not result from a deadlock.
	 */
	public <V> V transact(Callable<V> transaction)
	{
		// We retry for as long as it takes. There's no reason
		// why a transaction shouldn't eventually be able to acquire
		// the locks it needs.
		while (true)
		{
			beginTransaction();
			V result = null;
			try
			{
				result = transaction.call();
			}
			catch (Throwable t)
			{
                try { endTransaction(false); }
                catch (HGTransactionException tex) { tex.printStackTrace(System.err); }                			    
			    handleTxException(t); // will re-throw if we can't retry the transaction
//			    System.out.println("Retrying transaction");
			    continue;
			}
			try
			{
				endTransaction(true);
				return result;
			}  
			catch (Throwable t)
			{
                handleTxException(t); // will re-throw if we can't retry the transaction
//                System.out.println("Retrying transaction");
			}
		}
	}
}