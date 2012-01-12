/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.bdb;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HyperGraph;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.LockNotGrantedException;
import com.sleepycat.db.LockRequestMode;

/**
 * 
 * <p>
 * A <code>ReadWriteLock</code> implementation backed by the BerkeleyDB locking
 * mechanism. This implementation uses the current HGDB transaction as the BDB locker.
 * Only the <code>lock()</code>, <code>unlock()</code> and <code>tryLock()</code>
 * methods are actually supported for now. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class BDBTxLock implements ReadWriteLock
{
	private HyperGraph graph;
	private DatabaseEntry objectId;
	private BDBReadLock readLock = new BDBReadLock();
	private BDBWriteLock writeLock = new BDBWriteLock();	
	
	private int getLockerId()
	{
		try
		{
			return ((TransactionBDBImpl)graph.getTransactionManager().getContext().getCurrent().getStorageTransaction()).getBDBTransaction().getId();
		}
		catch (DatabaseException ex)
		{
			throw new HGException(ex);
		}
	}

	private Environment getEnv()
	{
		return ((TransactionBDBImpl)graph.getTransactionManager().getContext().getCurrent().getStorageTransaction()).getBDBEnvironment();		
	}
	
	private class BDBReadLock implements Lock
	{
		com.sleepycat.db.Lock lock = null;
		ThreadLocal<Integer> readCount = new ThreadLocal<Integer>() {
		    protected Integer initialValue() { return 0; }
		};
		
		BDBReadLock()
		{								
		}
		
		public synchronized void lock()
		{
			try
			{
				if (readCount.get() == 0)
					lock = getEnv().getLock(getLockerId(), false, objectId, LockRequestMode.READ);				
				readCount.set(readCount.get() + 1);				
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}
		}

		public void lockInterruptibly() throws InterruptedException
		{
			throw new UnsupportedOperationException();
		}

		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}

		public synchronized boolean tryLock()
		{
			try
			{
				if (readCount.get() == 0)
					lock = getEnv().getLock(getLockerId(), true, objectId, LockRequestMode.READ);
				if (lock != null)
				{
					readCount.set(readCount.get() + 1);
					return true;
				}
				else
					return false;
			}
			catch (LockNotGrantedException le)
			{
				return false;
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}
		}

		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
		{
			throw new UnsupportedOperationException();			
		}

		public synchronized void unlock()
		{
			try
			{
				if (lock != null)
				{
					int newcnt = readCount.get() - 1;
					if (newcnt < 0)
						throw new IllegalStateException("Lock already released.");
					else if (newcnt == 0)
					{
						getEnv().putLock(lock);					
						lock = null;
					}					
					readCount.set(newcnt);					
				}
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}			
		}		
	}
	
	private class BDBWriteLock implements Lock
	{
		com.sleepycat.db.Lock lock;
		ThreadLocal<Integer> writeCount = new ThreadLocal<Integer>() {
		    protected Integer initialValue() { return 0; }
		};
		
		BDBWriteLock()
		{
			writeCount.set(new Integer(0));				
		}
		
		public void lock()
		{
			try
			{				
				if (writeCount.get() == 0)
					lock = getEnv().getLock(getLockerId(), false, objectId, LockRequestMode.WRITE);
				writeCount.set(writeCount.get() + 1);
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}
		}

		public void lockInterruptibly() throws InterruptedException
		{
			throw new UnsupportedOperationException();
		}

		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}

		public boolean tryLock()
		{
			try
			{
				if (writeCount.get() == 0)
					lock = getEnv().getLock(getLockerId(), true, objectId, LockRequestMode.WRITE);
				if (lock != null)
				{					
					writeCount.set(writeCount.get() + 1);
					return true;
				}
				else
					return false;
			}
			catch (LockNotGrantedException le)
			{
				return false;
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}
		}

		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
		{
			throw new UnsupportedOperationException();			
		}

		public void unlock()
		{
			try
			{
				int newcnt = writeCount.get() - 1;
				if (newcnt < 0)
					throw new IllegalStateException("Lock already released.");
				else if (newcnt == 0)
				{
					getEnv().putLock(lock);
					lock = null;
				}
				writeCount.set(newcnt);
			}
			catch (DatabaseException ex)
			{
				throw new HGException(ex);
			}			
		}		
	}
	
	public BDBTxLock(HyperGraph graph, byte [] objectId)
	{
		this(graph, new DatabaseEntry(objectId));
	}
	
	public BDBTxLock(HyperGraph graph, DatabaseEntry objectId)
	{
		this.graph = graph;
		this.objectId = new DatabaseEntry();
		byte [] tmp = new byte[objectId.getData().length];
		System.arraycopy(objectId.getData(), 0, tmp, 0, tmp.length);
		this.objectId = new DatabaseEntry(tmp);
	}
	
	public Lock readLock()
	{
		return readLock;
	}

	public Lock writeLock()
	{
		return writeLock;
	}
	
	public HyperGraph getGraph()
	{
		return graph;
	}
	
	public byte [] getObjectId()
	{
		return objectId.getData();
	}
}
