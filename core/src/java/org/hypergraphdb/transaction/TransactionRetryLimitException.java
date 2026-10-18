package org.hypergraphdb.transaction;

import org.hypergraphdb.HGException;

/**
 * <p>
 * Thrown by {@link HGTransactionManager#transact(java.util.concurrent.Callable)} when a transaction keeps
 * failing with retryable conflicts and the {@link TransactionRetryPolicy#maxRetries()} of its retry policy
 * is exceeded. The cause is the conflict that made the last attempt fail.
 * </p>
 */
public class TransactionRetryLimitException extends HGException
{
    private static final long serialVersionUID = 1L;

    private final int retries;

    public TransactionRetryLimitException(int retries, Throwable lastConflict)
    {
        super("Transaction failed with a conflict after " + retries + " retries; giving up.", lastConflict);
        this.retries = retries;
    }

    /** The number of retries made before giving up. */
    public int getRetries()
    {
        return retries;
    }
}
