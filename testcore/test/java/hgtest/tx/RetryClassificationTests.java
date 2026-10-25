package hgtest.tx;

import static org.junit.Assert.*;

import org.hypergraphdb.HGException;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * Retryable conflicts usually reach callers wrapped in other exceptions (HGException, RuntimeException...).
 * Deciding whether a failed transaction can be retried must look at the whole cause chain.
 */
public class RetryClassificationTests extends HGTestBase
{
    static Throwable wrapped(Throwable t)
    {
        return new RuntimeException(new HGException(new RuntimeException(t)));
    }

    @Test
    public void transactionManagerLooksAtCauses()
    {
        assertTrue(graph.getTransactionManager().canRetry(new TransactionConflictException()));
        assertTrue(graph.getTransactionManager().canRetry(wrapped(new TransactionConflictException())));
        assertFalse(graph.getTransactionManager().canRetry(new IllegalStateException("not a conflict")));
        assertFalse(graph.getTransactionManager().canRetry(wrapped(new IllegalStateException("not a conflict"))));
    }

    @Test
    public void storageTransactionFactoryLooksAtCauses()
    {
        assertTrue(graph.getStore().getTransactionFactory().canRetryAfter(wrapped(new TransactionConflictException())));
        assertFalse(graph.getStore().getTransactionFactory().canRetryAfter(wrapped(new IllegalStateException())));
    }
}
