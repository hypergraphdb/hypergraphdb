package org.hypergraphdb.event;

import org.hypergraphdb.transaction.HGTransaction;

public class HGTransactionEndEvent extends HGEventBase
{
    private boolean success;
    private HGTransaction transaction;
    
    public HGTransactionEndEvent(HGTransaction transaction, boolean success)
    {
        this.transaction = transaction;
        this.success = success;
    }
    
    public HGTransaction getTransaction()
    {
        return transaction;
    }
    
    public boolean isSuccessful()
    {
        return success;
    }
}