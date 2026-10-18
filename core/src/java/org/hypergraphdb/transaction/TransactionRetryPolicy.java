package org.hypergraphdb.transaction;

/**
 * <p>
 * Decides what {@link HGTransactionManager#transact(java.util.concurrent.Callable)} does when a
 * transaction fails with a conflict the storage layer says can be retried (for example a lock timeout
 * or a deadlock): how long to wait before retrying, and how many times to retry at most.
 * </p>
 * <p>
 * The policy of a graph is set with
 * {@link org.hypergraphdb.HGConfiguration#setTransactionRetryPolicy(TransactionRetryPolicy)}, and can be
 * overridden for a single transaction with {@link HGTransactionConfig#setRetryPolicy(TransactionRetryPolicy)}.
 * The default is {@link RandomExponentialBackoff} with unlimited retries.
 * </p>
 */
public interface TransactionRetryPolicy
{
    /**
     * Return the number of milliseconds to wait before the given retry of a transaction
     * (1 for the first retry). 0 means retry immediately.
     */
    long waitMillis(int retry);

    /**
     * Return the maximum number of times a transaction is retried, or a negative number for no limit.
     * When the limit is exceeded, a {@link TransactionRetryLimitException} is thrown.
     */
    int maxRetries();
}
