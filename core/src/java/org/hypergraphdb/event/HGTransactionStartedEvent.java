package org.hypergraphdb.event;

import org.hypergraphdb.transaction.HGTransaction;

/**
 * An event triggered upon the start of every database transaction. Listen to
 * this event for example if you want to monitor all HyperGraphDB transactions.
 */
public class HGTransactionStartedEvent extends HGTransactionEvent
{
    public HGTransactionStartedEvent(HGTransaction transaction)
    {
        super(transaction);
    }
}
