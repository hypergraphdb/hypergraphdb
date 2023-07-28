/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage.mdbx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
//import org.hypergraphdb.handle.SequenceHandleFactory;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;
//import org.hypergraphdb.type.HGDataOutput;
//import org.hypergraphdb.util.HGSlf4jLogger;
import org.hypergraphdb.util.HGUtils;
//import org.hypergraphdb.util.IHGLogger;

import com.castortech.mdbxjni.Cursor;
import com.castortech.mdbxjni.MDBXException;
import com.castortech.mdbxjni.Env;
import com.castortech.mdbxjni.Transaction;
//import com.castortech.util.TimeUtils;

public class TransactionMdbxImpl implements HGStorageTransaction
{
//	private final IHGLogger log = new HGSlf4jLogger();

	private MdbxStorageImplementation storage;
//	private HGHandleFactory handleFactory;
	private Env env;
	private Transaction txn;
//	private long lastId = -1;
//	private Set<MdbxTxCursor> mdbxCursors = ConcurrentHashMap.newKeySet();
	private List<MdbxTxCursor> mdbxCursors = Collections
			.synchronizedList(new ArrayList<>());
	private boolean aborting = false;
	private int syncFrequency = 0;
	private boolean syncForce = false;
	private boolean readOnly = false;

	public static final TransactionMdbxImpl nullTransaction()
	{
		return new TransactionMdbxImpl(null, null, null, null, true);
	}

	public TransactionMdbxImpl(MdbxStorageImplementation storage,
			HGHandleFactory handleFactory, Transaction txn, Env env,
			boolean readOnly)
	{
		this.storage = storage;
//		this.handleFactory = handleFactory;
		this.env = env;
		this.txn = txn;
		this.readOnly = readOnly;

		if (storage != null)
		{
			syncFrequency = storage.getConfiguration().getSyncFrequency();
			syncForce = storage.getConfiguration().isSyncForce();
		}
	}

	public Env getDbEnvironment()
	{
		return env;
	}

	public Transaction getDbTransaction()
	{
		return txn;
	}

	@Override
	public void commit() throws HGTransactionException
	{
//		HGUtils.checkInterrupted();
		try
		{
//			if (lastId != -1)
//				storeNext();

			closeTransaction();

//			System.out.println("starting mdbx commit2 at:" + System.currentTimeMillis());
			if (txn != null)
				txn.commit();
//			System.out.println("starting mdbx commit3 at:" + System.currentTimeMillis());

			if (!readOnly)
			{
				long commitCnt = storage.getNextCommitCount();
				if (syncFrequency != 0 && commitCnt % syncFrequency == 0)
				{
//					log.info("synching {}", commitCnt);
//					log.info(storage.getStats(Collections.emptyMap()));
					env.sync(syncForce);
				}
			}
		}
		catch (MDBXException ex)
		{
			throw new HGTransactionException("Failed to commit transaction",
					ex);
		}
	}

	@Override
	public void abort() throws HGTransactionException
	{
		try
		{
			aborting = true;
			closeTransaction();

			if (txn != null)
				txn.abort();
		}
		catch (MDBXException ex)
		{
			throw new HGTransactionException("Failed to abort transaction", ex);
		}
	}

	public MdbxTxCursor attachCursor(Cursor cursor)
	{
		if (txn == null)
			return new MdbxTxCursor(cursor, null);
		MdbxTxCursor c = new MdbxTxCursor(cursor, this);
		mdbxCursors.add(c);
		return c;
	}

	void removeCursor(MdbxTxCursor c)
	{
		if (!aborting)
			mdbxCursors.remove(c);
	}

//	private void storeNext()
//	{
//		final HGDataOutput out = storage.newDataOutput();
//		final String nextHandleAsString = String.valueOf(lastId);
//		out.writeString(nextHandleAsString);
//		storage.storeNextId(txn, getNextStorageHandle(), out.toByteArray());
//	}
//
//	private HGPersistentHandle getNextStorageHandle()
//	{
//		if (handleFactory instanceof SequenceHandleFactory)
//		{
//			return ((SequenceHandleFactory) handleFactory).getNexthandle();
//		}
//		else
//		{
//			throw new IllegalStateException(
//					"Incompatible handle Factory type " + handleFactory);
//		}
//	}

	private void closeTransaction()
	{ // release resources
		// Since 'close' removes from the set, we need to clone first to
		// avoid a ConcurrentModificationException
		if (!mdbxCursors.isEmpty())
		{
			List<MdbxTxCursor> tmp = new ArrayList<>();
			tmp.addAll(mdbxCursors);

//			log.debug("Commit going to close {} cursors", mdbxCursors.size());
//			long elapsed = System.currentTimeMillis();
			int cnt = txn.releaseCursors();
//			for (int i = tmp.size() -1; i >= 0; i--) {
//				MdbxTxCursor c = tmp.get(i);
//				try {
//					c.close();
//				}
//				catch (Exception e) {
//					log.error("Exception Occurred", e);
//				}
//			}

//			log.debug("Close cursors ({}) took {}", cnt,
//					TimeUtils.elapsedSince(elapsed));
			tmp.forEach(HGUtils::closeNoException);
		}
	}
}