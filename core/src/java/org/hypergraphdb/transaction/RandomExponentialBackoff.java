package org.hypergraphdb.transaction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * <p>
 * The default {@link TransactionRetryPolicy}: before retry <i>n</i>, wait a random time between 0 and
 * <code>min(maxMillis, initialMillis * 2^(n-1))</code>. The randomness keeps transactions that conflicted
 * with each other (for example the two sides of a deadlock detected by the storage layer) from retrying in
 * lockstep, and the growing bound spreads out retries under sustained contention.
 * </p>
 */
public class RandomExponentialBackoff implements TransactionRetryPolicy
{
    /** The defaults: 2ms, doubling up to 100ms, unlimited retries. */
    public static final long DEFAULT_INITIAL_MILLIS = 2;
    public static final long DEFAULT_MAX_MILLIS = 100;

    private final long initialMillis;
    private final long maxMillis;
    private final int maxRetries;

    /** Retry immediately, without limit (the behavior before retry policies existed). */
    public static RandomExponentialBackoff immediate()
    {
        return new RandomExponentialBackoff(0, 0, -1);
    }

    /** The default policy: 2ms doubling up to 100ms, unlimited retries. */
    public RandomExponentialBackoff()
    {
        this(DEFAULT_INITIAL_MILLIS, DEFAULT_MAX_MILLIS, -1);
    }

    /**
     * @param initialMillis The bound of the random wait before the first retry; 0 to never wait.
     * @param maxMillis The largest bound the doubling can reach.
     * @param maxRetries The maximum number of retries, or a negative number for no limit.
     */
    public RandomExponentialBackoff(long initialMillis, long maxMillis, int maxRetries)
    {
        if (initialMillis < 0 || maxMillis < 0)
            throw new IllegalArgumentException("Negative wait: initialMillis=" + initialMillis + ", maxMillis=" + maxMillis);
        this.initialMillis = initialMillis;
        this.maxMillis = Math.max(initialMillis, maxMillis);
        this.maxRetries = maxRetries;
    }

    /** The upper bound of the random wait before the given retry. */
    public long boundMillis(int retry)
    {
        if (initialMillis == 0 || retry < 1)
            return 0;
        int doublings = Math.min(retry - 1, 62);
        long bound = initialMillis;
        for (int i = 0; i < doublings && bound < maxMillis; i++)
            bound <<= 1;
        return Math.min(bound, maxMillis);
    }

    public long waitMillis(int retry)
    {
        long bound = boundMillis(retry);
        return bound == 0 ? 0 : ThreadLocalRandom.current().nextLong(bound + 1);
    }

    public int maxRetries()
    {
        return maxRetries;
    }

    public long getInitialMillis()
    {
        return initialMillis;
    }

    public long getMaxMillis()
    {
        return maxMillis;
    }

    public String toString()
    {
        return "RandomExponentialBackoff(initialMillis=" + initialMillis + ", maxMillis=" + maxMillis
               + ", maxRetries=" + (maxRetries < 0 ? "unlimited" : String.valueOf(maxRetries)) + ")";
    }
}
