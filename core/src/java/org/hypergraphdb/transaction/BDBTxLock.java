package org.hypergraphdb.transaction;

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
			return ((TransactionBDBImpl)graph.getTransactionManager().getContext().getCurrent()).getBDBTransaction().getId();
		}
		catch (DatabaseException ex)
		{
			throw new HGException(ex);
		}
	}

	private Environment getEnv()
	{
		return ((TransactionBDBImpl)graph.getTransactionManager().getContext().getCurrent()).getBDBEnvironment();		
	}
	
	private class BDBReadLock implements Lock
	{
		com.sleepycat.db.Lock lock;
		
		BDBReadLock()
		{
		}
		
		public void lock()
		{
			try
			{
				lock = getEnv().getLock(getLockerId(), false, objectId, LockRequestMode.READ);
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
				lock = getEnv().getLock(getLockerId(), true, objectId, LockRequestMode.READ);
				return true;
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
				getEnv().putLock(lock);
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
		
		BDBWriteLock()
		{
		}
		
		public void lock()
		{
			try
			{
				lock = getEnv().getLock(getLockerId(), false, objectId, LockRequestMode.WRITE);
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
				lock = getEnv().getLock(getLockerId(), true, objectId, LockRequestMode.WRITE);
				return true;
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
				getEnv().putLock(lock);
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