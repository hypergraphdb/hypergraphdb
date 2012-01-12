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
 * A <code>HGTransactionException</code> is thrown from within the transaction
 * handling of HyperGraph. When such an exception is thrown during an operation
 * on a particular transaction, that transaction must be assumed invalid. Thus,
 * when <code>HGTransactionException</code> is caught, this means that the current
 * transaction has become invalid and cannot not be aborted. Similarly, when
 * caught a <code>HGTransactionException</code> should never be rethrown which 
 * could prevent the freeing of parent transaction up the call stack. If you need
 * to propagate a <code>HGTransactionException</code>, you must wrap it in some
 * other exception (e.g. <code>RuntimeException</code>).
 * </p>
 *
 * <p>
 * <code>HGTransactionException</code>s mean one of two things: 
 * <ol>
 * <li>A bug in the client
 * code that's attempting to commit/abort an unexisting or an already completed
 * transaction.</li>
 * <li>A low-level problem in the underlying storage mechanism (e.g. BerkeleyDB)
 * that should also be investigated separately, possibly after running a DB recovery.
 * </li>
 * </ol> 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGTransactionException extends Exception
{
    static final long serialVersionUID = -1;
    
    public HGTransactionException(String msg)
    {
        super(msg);
    }
    
    public HGTransactionException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    public HGTransactionException(Throwable cause)
    {
        super(cause);
    }
}