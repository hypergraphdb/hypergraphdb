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
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * The <code>HGTransactionManager</code> handles transactional activity for a
 * single HyperGraph instance. You can obtain the transaction manager for a
 * HyperGraph by calling its <code>getTransactionManager</code> method.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGTransactionManager
{
	private HyperGraph graph;
	private HGTransactionFactory factory;
	private ThreadLocal<HGTransactionContext> tcontext = new ThreadLocal<HGTransactionContext>();
	private boolean enabled = true;

	volatile ActiveTransactionsRecord mostRecentRecord = new ActiveTransactionsRecord(
			0, null);

	final ReentrantLock COMMIT_LOCK = new ReentrantLock(true);

	TxMonitor txMonitor = null;

	/**
	 * <p>
	 * Return <code>true</code> if the transaction are enabled and
	 * <code>false</code> otherwise.
	 * </p>
	 */
	public boolean isEnabled()
	{
		return enabled;
	}

	/**
	 * <p>
	 * Enable or disable transactions. Note that all current transactions will
	 * be silently aborted so make sure any pending transactions are completed
	 * before calling this method.
	 * </p>
	 * 
	 * @param enabled
	 *            <code>true</code> if transaction must be henceforth enabled
	 *            and <code>false</code> otherwise.
	 */
	public void setEnabled(boolean enabled)
	{
		this.enabled = enabled;
	}

	/**
	 * <p>
	 * Enable transactions - equivalent to <code>setEnabled(true)</code>.
	 * </p>
	 */
	public void enable()
	{
		setEnabled(true);
	}

	/**
	 * <p>
	 * Disable transactions - equivalent to <code>setEnabled(false)</code>
	 * </p>
	 */
	public void disable()
	{
		setEnabled(false);
	}

	/**
	 * <p>
	 * Return the <code>HGTransactionContext</code> instance associated with the
	 * current thread.
	 * </p>
	 */
	public HGTransactionContext getContext()
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
	 * Construct a new transaction manager with the given storage transaction
	 * factory. This method is normally called only internally. To obtain the
	 * transaction manager bound to a HyperGraph, use
	 * <code>HyperGraph.getTransactionManager</code>.
	 * </p>
	 * 
	 * @param factory
	 *            The <code>HGTransactionFactory</code> responsible for
	 *            fabricating new transactions.
	 */
	public HGTransactionManager(HGTransactionFactory factory)
	{
		this.factory = factory;

	}

	/**
	 * <p>
	 * Set the {@link HyperGraph} instance associated with this
	 * <code>HGTransactionManager</code>. <strong>Do not call - used internally
	 * during initialization.
	 * </p>
	 */
	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
		this.txMonitor = new TxMonitor(this);
	}

	/**
	 * <p>
	 * Return the {@link HyperGraph} instance associated with this
	 * <code>HGTransactionManager</code>.
	 * </p>
	 */
	public HyperGraph getHyperGraph()
	{
		return graph;
	}

	/**
	 * <p>
	 * Attach the given transaction context to the current thread. This method
	 * is normally called in a server environment. By default, a transaction
	 * context will be created and bound to a thread if need be, every time a
	 * new transaction is requested. So when HyperGraph is embedded in a client
	 * application, there is no need to explicitly attach/detach contexts to
	 * threads. However, in an environment using thread pooling such as is
	 * common in servers where a single transaction can span multiple requests,
	 * use the <code>threadAttach</code> and <code>threadDetach</code> methods
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
	 * Detach the transaction context bound to the current thread. This method
	 * is normally called in a server environment. By default, a transaction
	 * context will be created and bound to a thread if need be, every time a
	 * new transaction is requested. So when HyperGraph is embedded in a client
	 * application, there is no need to explicitly attach/detach contexts to
	 * threads.
	 * </p>
	 * 
	 * <p>
	 * <strong>IMPORTANT NOTE:</strong> when managing transaction contexts
	 * explicitly, you are responsible for closing all pending transactions in
	 * the context before disposing of it. This is done by invoking the
	 * <code>HGTransactionContext.endAll</code> method.
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
	 * @param The
	 *            parent <code>HGTransaction</code> - if null, a top-level
	 *            transaction object is returned.
	 * @return The newly created transaction.
	 */
	HGTransaction createTransaction(HGTransaction parent,
			HGTransactionConfig config)
	{
		HGStorageTransaction storageTx = config.isNoStorage() || !enabled ? null
				: factory.createTransaction(getContext(), config, parent);
		ActiveTransactionsRecord activeRecord = mostRecentRecord
				.getRecordForNewTransaction();
		if (enabled)
		{
			HGTransaction result = new HGTransaction(getContext(), parent,
					activeRecord, storageTx, config.isReadonly());
			return result;
		}
		else
			return new HGTransaction(getContext(), parent, activeRecord,
					new VanillaTransaction(), config.isReadonly());
	}

	/**
	 * <p>
	 * Begin a new transaction in the current transaction context. If there's no
	 * transaction context bound to the active thread, one will be created.
	 * </p>
	 */
	public void beginTransaction()
	{
		beginTransaction(HGTransactionConfig.DEFAULT);
	}

	/**
	 * <p>
	 * Begin a new transaction in the current transaction context. If there's no
	 * transaction context bound to the active thread, one will be created.
	 * </p>
	 * 
	 * @param config
	 *            A {@link HGTransactionConfig} instance holding configuration
	 *            parameters for the newly created transaction.
	 */
	public void beginTransaction(HGTransactionConfig config)
	{
		getContext().beginTransaction(config);
	}

	/**
	 * <p>
	 * Terminate the currently active transaction. The transaction will be
	 * aborted or committed based on the <code>success</code> flag (abort when
	 * false and commit when true).
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
			throw new HGException(
					"Attempt to end a transaction with no transaction context currently active.");
		else
			ctx.endTransaction(success);
	}

	/**
	 * <p>
	 * Commit the current transaction by calling
	 * <code>endTransaction(true)</code>. Wrap the possible
	 * <code>HGTransactionException</code> in a <code>HGException</code>.
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
	 * Abort the current transaction by calling
	 * <code>endTransaction(false)</code>. Wrap the possible
	 * <code>HGTransactionException</code> in a <code>HGException</code>.
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
	 * Equivalent to
	 * <code>ensureTransaction(transaction, HGTransactionConfig.DEFAULT)</code>.
	 * </p>
	 */
	public <V> V ensureTransaction(Callable<V> transaction)
	{
		return ensureTransaction(transaction, HGTransactionConfig.DEFAULT);
	}

	/**
	 * <p>
	 * Perform a unit of work encapsulated as a transaction and return the
	 * result. This method will reuse the currently active transaction if there
	 * is one or create a new transaction otherwise.
	 * </p>
	 * 
	 * @param <V>
	 *            The type of the return value.
	 * @param transaction
	 *            The transaction process encapsulated as a
	 *            <code>Callable</code> instance.
	 * @param config
	 *            The configuration of this transaction - note that if there's a
	 *            current transaction in effect, this configuration parameter
	 *            will be ignored as no new transaction will be created.
	 * @return The result of <code>transaction.call()</code>.
	 * @throws The
	 *             method will (re)throw any exception that does not result from
	 *             a deadlock.
	 */
	public <V> V ensureTransaction(Callable<V> transaction,
			HGTransactionConfig config)
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
			return transact(transaction, config);
	}

	void handleTxException(Throwable t)
	{
		// If there is a DeadlockException at the root of this, we have to
		// simply abort
		// the transaction and try again.
		boolean retry = false;
		for (Throwable cause = t; cause != null; cause = cause.getCause())
			if (factory.canRetryAfter(cause))
			{
				retry = true;
				break;
			}

		if (!retry)
		{
			if (t instanceof RuntimeException)
				throw (RuntimeException) t;
			else
				throw new HGException(t);
		}
	}

	/**
	 * <p>
	 * Equivalent to
	 * <code>transact(transaction, HGTransactionConfig.DEFAULT)</code>.
	 * </p>
	 */
	public <V> V transact(Callable<V> transaction)
	{
		return transact(transaction, HGTransactionConfig.DEFAULT);
	}

	/**
	 * <p>
	 * Perform a unit of work encapsulated as a transaction and return the
	 * result. This method explicitly allows deadlock (or write conflicts) to
	 * occur and it will re-attempt the transaction in such a case indefinitely.
	 * In order for the transaction to eventually complete, the underlying
	 * transactional system must be configured to be fair or to prioritize
	 * transaction randomly (which is the default behavior).
	 * </p>
	 * 
	 * <p>
	 * If the <code>transaction.call()</code> returns without an exception, but
	 * the underlying database transaction has already been committed or
	 * aborted, this method return without doing anything further. Otherwise,
	 * upon a normal return from <code>transaction.call()</code> it will try to
	 * commit and re-try indefinitely if the commit fails.
	 * </p>
	 * 
	 * <p>
	 * It is important that the <code>transaction.call()</code> doesn't leave
	 * any open nested transactions on the transaction stack.
	 * </p>
	 * 
	 * @param <V>
	 *            The type of the return value.
	 * @param transaction
	 *            The transaction process encapsulated as a
	 *            <code>Callable</code> instance.
	 * @param config
	 *            The transaction configuration parameters.
	 * @return The result of <code>transaction.call()</code> or
	 *         <code>null</code> if the transaction was aborted by the
	 *         application by throwing a {@link HGUserAbortException}.
	 * @throws The
	 *             method will (re)throw any exception that does not result from
	 *             a deadlock.
	 */
	public <V> V transact(Callable<V> transaction, HGTransactionConfig config)
	{
		// If we are monitoring, we have the same logic below but annotated with
		// some monitoring operations in the TxMonitoringRunner. Should the
		// logic
		// of that retry loop change, due to a bug or whatever reason, the
		// change
		// has to be replicated to the monitoring version as well.
		if (this.monitor().enabled())
		{
			return this.monitor().transact(
					Thread.currentThread().getName()
							+ System.currentTimeMillis(), // unique name
					transaction, config);
		}

		// We retry for as long as it takes. There's no reason
		// why a transaction shouldn't eventually be able to acquire
		// the locks it needs.
		while (true)
		{
			beginTransaction(config);
			V result = null;
			try
			{
				result = transaction.call();
			}
			catch (HGUserAbortException ex)
			{
				try
				{
					endTransaction(false);
				}
				catch (HGTransactionException tex)
				{
					tex.printStackTrace(System.err);
				}
				return null;
			}
			catch (Throwable t)
			{
				try
				{
					endTransaction(false);
				}
				catch (HGTransactionException tex)
				{
					tex.printStackTrace(System.err);
				}
				if (HGUtils.getRootCause(
						t) instanceof TransactionIsReadonlyException
						&& config.isWriteUpgradable())
				{
					config = HGTransactionConfig.DEFAULT;
				}
				else
				{
					handleTxException(t); // will re-throw if we can't retry the
											// transaction
				}
				continue;
			}
			try
			{
				endTransaction(true);
				return result;
			}
			catch (Throwable t)
			{
				if (HGUtils.getRootCause(
						t) instanceof TransactionIsReadonlyException
						&& config.isWriteUpgradable())
				{
					config = HGTransactionConfig.DEFAULT;
				}
				else
				{
					handleTxException(t); // will re-throw if we can't retry the
											// transaction
				}
			}
		}
	}

	/**
	 * Return {@link TxMonitor} if the
	 * {@link org.hypergraphdb.HGConfiguration#getMonitorTransactions()} is
	 * <code>true</code> and <code>null</code> otherwise.
	 */
	public TransactionMonitor monitor()
	{
		return this.txMonitor;
	}

}