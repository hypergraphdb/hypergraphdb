package org.hypergraphdb.event;

import org.hypergraphdb.transaction.HGTransaction;

public class HGTransactionStartedEvent extends HGEventBase
{
    private HGTransaction transaction;
    
    public HGTransactionStartedEvent(HGTransaction transaction)
    {
        this.transaction = transaction;
    }
    
    public HGTransaction getTransaction()
    {
        return transaction;
    }
}
