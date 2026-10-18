package hgtest.tx;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.RandomExponentialBackoff;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.hypergraphdb.transaction.TransactionRetryLimitException;
import org.hypergraphdb.transaction.TransactionRetryPolicy;
import org.hypergraphdb.util.HGUtils;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * How {@link HGTransactionManager#transact(Callable)} retries transactions that fail with a
 * retryable conflict, as decided by a {@link TransactionRetryPolicy}.
 */
public class TransactionRetryPolicyTests extends HGTestBase
{
    /** A transaction body that fails with a retryable conflict a given number of times, then succeeds. */
    static class Conflicting implements Callable<String>
    {
        final AtomicInteger attempts = new AtomicInteger();
        final int failures;

        Conflicting(int failures) { this.failures = failures; }

        public String call()
        {
            if (attempts.incrementAndGet() <= failures)
                throw new TransactionConflictException();
            return "done";
        }
    }

    static HGTransactionConfig withPolicy(TransactionRetryPolicy policy)
    {
        return new HGTransactionConfig().setRetryPolicy(policy);
    }

    @Test
    public void defaultPolicyBounds()
    {
        RandomExponentialBackoff p = new RandomExponentialBackoff();
        assertEquals(-1, p.maxRetries());
        assertEquals(2, p.boundMillis(1));
        assertEquals(4, p.boundMillis(2));
        assertEquals(8, p.boundMillis(3));
        assertEquals(64, p.boundMillis(6));
        assertEquals(100, p.boundMillis(7));
        assertEquals(100, p.boundMillis(1000));
        for (int retry = 1; retry < 20; retry++)
            for (int sample = 0; sample < 50; sample++)
            {
                long w = p.waitMillis(retry);
                assertTrue("wait " + w + " for retry " + retry, w >= 0 && w <= p.boundMillis(retry));
            }
    }

    @Test
    public void immediatePolicyNeverWaits()
    {
        RandomExponentialBackoff p = RandomExponentialBackoff.immediate();
        for (int retry = 1; retry < 100; retry++)
            assertEquals(0, p.waitMillis(retry));
        assertEquals(-1, p.maxRetries());
    }

    @Test
    public void retryLimitExceeded()
    {
        Conflicting body = new Conflicting(Integer.MAX_VALUE);
        try
        {
            graph.getTransactionManager().transact(body, withPolicy(new RandomExponentialBackoff(0, 0, 3)));
            fail("expected TransactionRetryLimitException");
        }
        catch (TransactionRetryLimitException ex)
        {
            assertEquals(3, ex.getRetries());
            assertTrue("cause is the last conflict", ex.getCause() instanceof TransactionConflictException);
        }
        assertEquals("one attempt plus three retries", 4, body.attempts.get());
    }

    @Test
    public void zeroRetriesMeansSingleAttempt()
    {
        Conflicting body = new Conflicting(Integer.MAX_VALUE);
        try
        {
            graph.getTransactionManager().transact(body, withPolicy(new RandomExponentialBackoff(0, 0, 0)));
            fail("expected TransactionRetryLimitException");
        }
        catch (TransactionRetryLimitException ex)
        {
            assertEquals(0, ex.getRetries());
        }
        assertEquals(1, body.attempts.get());
    }

    @Test
    public void succeedsWithinLimit()
    {
        Conflicting body = new Conflicting(2);
        assertEquals("done", graph.getTransactionManager().transact(body, withPolicy(new RandomExponentialBackoff(0, 0, 2))));
        assertEquals(3, body.attempts.get());
    }

    @Test
    public void waitsAsThePolicySays()
    {
        final List<Integer> retries = Collections.synchronizedList(new ArrayList<Integer>());
        TransactionRetryPolicy policy = new TransactionRetryPolicy() {
            public long waitMillis(int retry) { retries.add(retry); return 30; }
            public int maxRetries() { return -1; }
        };
        Conflicting body = new Conflicting(3);
        long start = System.nanoTime();
        assertEquals("done", graph.getTransactionManager().transact(body, withPolicy(policy)));
        long elapsedMillis = (System.nanoTime() - start) / 1000000;
        assertEquals(listOf(1, 2, 3), retries);
        assertTrue("waited about 3 x 30ms, took " + elapsedMillis + "ms", elapsedMillis >= 90);
    }

    @Test
    public void managerPolicyAppliesWhenTransactionHasNone()
    {
        HGTransactionManager manager = graph.getTransactionManager();
        TransactionRetryPolicy previous = manager.getRetryPolicy();
        manager.setRetryPolicy(new RandomExponentialBackoff(0, 0, 2));
        try
        {
            Conflicting body = new Conflicting(Integer.MAX_VALUE);
            try
            {
                manager.transact(body);
                fail("expected TransactionRetryLimitException");
            }
            catch (TransactionRetryLimitException ex)
            {
                assertEquals(2, ex.getRetries());
            }
            assertEquals(3, body.attempts.get());
            // A transaction's own policy takes precedence over the manager's.
            Conflicting other = new Conflicting(5);
            assertEquals("done", manager.transact(other, withPolicy(new RandomExponentialBackoff(0, 0, 5))));
            assertEquals(6, other.attempts.get());
        }
        finally
        {
            manager.setRetryPolicy(previous);
        }
    }

    @Test
    public void graphConfigurationSetsThePolicy() throws Exception
    {
        File location = Files.createTempDirectory("hgtest-retry-policy").toFile();
        TransactionRetryPolicy policy = new RandomExponentialBackoff(0, 0, 1);
        HGConfiguration config = new HGConfiguration().setTransactionRetryPolicy(policy);
        HyperGraph other = HGEnvironment.get(location.getAbsolutePath(), config);
        try
        {
            assertSame(policy, other.getTransactionManager().getRetryPolicy());
            Conflicting body = new Conflicting(Integer.MAX_VALUE);
            try
            {
                other.getTransactionManager().transact(body);
                fail("expected TransactionRetryLimitException");
            }
            catch (TransactionRetryLimitException ex)
            {
                assertEquals(1, ex.getRetries());
            }
            assertEquals(2, body.attempts.get());
        }
        finally
        {
            other.close();
            HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
        }
    }

    @Test
    public void nonRetryableExceptionsAreNotRetried()
    {
        final AtomicInteger attempts = new AtomicInteger();
        try
        {
            graph.getTransactionManager().transact(new Callable<Object>() {
                public Object call()
                {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("not a conflict");
                }
            }, withPolicy(new RandomExponentialBackoff(0, 0, 5)));
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException expected)
        {
        }
        assertEquals(1, attempts.get());
    }

    private static List<Integer> listOf(Integer... values)
    {
        List<Integer> L = new ArrayList<Integer>();
        Collections.addAll(L, values);
        return L;
    }
}
