/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.transaction.HGTransaction;

/**
 * An event pertaining to a transaction. 
 *
 */
public abstract class HGTransactionEvent extends HGEventBase
{
    private HGTransaction transaction;
    
    public HGTransactionEvent(HGTransaction transaction)
    {
        this.transaction = transaction;
    }
    
    /**
     * Return the transaction object representing the transaction that just ended.
     */
    public HGTransaction getTransaction()
    {
        return transaction;
    }
}