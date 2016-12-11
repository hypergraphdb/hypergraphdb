/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.fusesource.hawtjni.runtime.Callback;
import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.JNI;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.Transaction;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.LongHandleFactory;
import org.hypergraphdb.storage.lmdb.type.HGDataOutput;
import org.hypergraphdb.storage.lmdb.type.util.ByteArrayComparator;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;

public class TransactionLmdbImpl implements HGStorageTransaction
{
	private LmdbStorageImplementation storeImpl;
	private HGHandleFactory handleFactory;
	private Env env;
	private Transaction t;
	private long lastId = -1;
	private Set<LmdbTxCursor> lmdbCursors = new HashSet<LmdbTxCursor>();
	private boolean aborting = false;
	private int syncFrequency = 0;
	private boolean syncForce = false;
	private boolean readOnly = false;
	private HashMap<Long, Callback> comparatorCallbacks = new HashMap<Long, Callback>();
			
	public static final TransactionLmdbImpl nullTransaction()
	{
		return new TransactionLmdbImpl(null, null, null, null, true);
	}

	public TransactionLmdbImpl(LmdbStorageImplementation storeImpl,
			HGHandleFactory handleFactory, Transaction t, Env env,
			boolean readOnly)
	{
		this.storeImpl = storeImpl;
		this.handleFactory = handleFactory;
		this.env = env;
		this.t = t;
		this.readOnly = readOnly;

		if (storeImpl != null)
		{
			syncFrequency = storeImpl.getConfiguration().getSyncFrequency();
			syncForce = storeImpl.getConfiguration().isSyncForce();
		}
	}

	public void ensureComparator(Long dbpointer, Comparator<byte[]> comparator)
	{
		if (this.comparatorCallbacks.containsKey(dbpointer)) return;
		Callback callback = new Callback(new ByteArrayComparator(comparator), "compare", 2);
		JNI.mdb_set_compare(this.t.pointer(), dbpointer, callback.getAddress());
		this.comparatorCallbacks.put(dbpointer, callback);
	}
	
	public Env getDbEnvironment()
	{
		return env;
	}

	public Transaction getDbTransaction()
	{
		return t;
	}

	public long getLastId()
	{
		return lastId;
	}

	public void setLastId(long lastId)
	{
		if (t != null && lastId > this.lastId)
			this.lastId = lastId;
	}

	public void commit() throws HGTransactionException
	{
		try
		{
			for (Callback callback : comparatorCallbacks.values())
				callback.dispose();
			// Since 'close' removes from the set, we need to clone first to
			// avoid a ConcurrentModificationException
			Set<LmdbTxCursor> tmp = new HashSet<>();
			tmp.addAll(lmdbCursors);

			for (LmdbTxCursor c : tmp)
				try
				{
					c.close();
				}
				catch (Throwable t)
				{
					System.err.println(t);
				}

			if (lastId != -1)
				storeNext();

			// System.out.println("starting lmdb commit2 at:" +
			// System.currentTimeMillis());
			if (t != null)
				t.commit();
			// System.out.println("starting lmdb commit3 at:" +
			// System.currentTimeMillis());

			if (!readOnly)
			{
				long commitCnt = storeImpl.getNextCommitCount();
				if (syncFrequency != 0 && commitCnt % syncFrequency == 0)
				{
					System.out.println("synching" + commitCnt);
					System.out.println(storeImpl.getStats());
					env.sync(syncForce);
				}
			}
		}
		catch (LMDBException ex)
		{
			throw new HGTransactionException("Failed to commit transaction",
					ex);
		}
	}

	public void abort() throws HGTransactionException
	{
		try
		{
			for (Callback callback : comparatorCallbacks.values())
				callback.dispose();
			
			aborting = true;
			// Since 'close' removes from the set, we need to clone first to
			// avoid a ConcurrentModificationException
			Set<LmdbTxCursor> tmp = new HashSet<LmdbTxCursor>();
			tmp.addAll(lmdbCursors);

			for (LmdbTxCursor c : tmp)
				try
				{
					c.close();
				}
				catch (Throwable t)
				{
					System.err.println(t);
				}

			if (t != null)
				t.abort();
		}
		catch (LMDBException ex)
		{
			throw new HGTransactionException("Failed to abort transaction", ex);
		}
	}

	public LmdbTxCursor attachCursor(Cursor cursor)
	{
		if (t == null)
			return new LmdbTxCursor(cursor, null);
		LmdbTxCursor c = new LmdbTxCursor(cursor, this);
		lmdbCursors.add(c);
		return c;
	}

	void removeCursor(LmdbTxCursor c)
	{
		if (!aborting)
			lmdbCursors.remove(c);
	}

	private void storeNext()
	{
		final HGDataOutput out = LmdbDataOutput.getInstance();
		final String nextHandleAsString = String.valueOf(lastId);
		out.writeString(nextHandleAsString);
		storeImpl.storeNextId(t, getNextStorageHandle(), out.toByteArray());
	}

	private HGPersistentHandle getNextStorageHandle()
	{
		if (handleFactory instanceof LongHandleFactory)
		{
			return ((LongHandleFactory) handleFactory).makeHandle();
		}
		else
		{
			throw new IllegalStateException(
					"Incompatible handle Factory type " + handleFactory);
		}
	}
}