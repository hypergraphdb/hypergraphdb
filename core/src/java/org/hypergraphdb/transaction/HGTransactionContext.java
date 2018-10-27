/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
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
 * {@link HGTransactionManager#threadAttach(HGTransactionContext)} and 
 * {@link HGTransactionManager#threadDetach()} methods.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGTransactionContext
{
    HGTransactionManager getManager();    
	HGTransaction getCurrent();
	void beginTransaction(HGTransactionConfig config);
	void endTransaction(boolean success) throws HGTransactionException;
//	void endAll(boolean success) throws HGTransactionException;
}