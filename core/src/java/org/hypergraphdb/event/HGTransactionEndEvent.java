package org.hypergraphdb.event;

import org.hypergraphdb.transaction.HGTransaction;

/**
 * An event triggered upon completion (successful or not) of each transaction. 
 * The <code>success</code> indicates whether the transaction was successful.
 * Listen to this event for example if you want to monitor all transactions
 * occurring in the database.
 *
 */
public class HGTransactionEndEvent extends HGEventBase
{
    private boolean success;
    private HGTransaction transaction;
    
    public HGTransactionEndEvent(HGTransaction transaction, boolean success)
    {
        this.transaction = transaction;
        this.success = success;
    }
    
    /**
     * Return the transaction object representing the transaction that just ended.
     */
    public HGTransaction getTransaction()
    {
        return transaction;
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