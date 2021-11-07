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
 * An event triggered upon completion (successful or not) of each transaction. 
 * The <code>success</code> indicates whether the transaction was successful.
 * Listen to this event for example if you want to monitor all transactions
 * occurring in the database.
 *
 */
public class HGTransactionEndEvent extends HGTransactionEvent
{
    private boolean success;
    
    public HGTransactionEndEvent(HGTransaction transaction, boolean success)
    {
        super(transaction);
        this.success = success;
    }
    
    /**
     * Return <code>true</code> if the transaction completed successfully and <code>false</code>
     * otherwise.
     */
    public boolean isSuccessful()
    {
        return success;
    }
}