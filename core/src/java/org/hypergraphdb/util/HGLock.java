/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HyperGraph;

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
 * @deprecated This class hasn't been in use since the introduction of MVCC which is
 * now how concurrent access to RAM data structures is handled.
 */
public class HGLock implements ReadWriteLock
{
	private ReentrantReadWriteLock defaultLock = null;
//	private BDBTxLock txLock = null;
	
	ReadWriteLock getApplicableLock()
	{
//		HGTransaction tx = txLock.getGraph().getTransactionManager().getContext().getCurrent(); 
//		if (tx == null || ! (tx.getStorageTransaction() instanceof TransactionBDBImpl))
//		{
//			if (defaultLock == null)
//				defaultLock = new ReentrantReadWriteLock();
//			return defaultLock;
//		}
//		else
//			return txLock;
	    return defaultLock;
	}
	
	public HGLock(HyperGraph graph, byte [] objectId)
	{
//		txLock = new BDBTxLock(graph, objectId);
	    throw new UnsupportedOperationException("The HGLock is deprecated and will be removed very soon - use the BDBTxLock directly from the refactored storage package if you need it.");
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
