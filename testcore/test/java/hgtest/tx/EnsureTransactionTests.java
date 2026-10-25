package hgtest.tx;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGException;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * Exceptions thrown inside ensureTransaction must reach the caller unchanged when they are unchecked,
 * whether or not a transaction is already active, instead of gaining a RuntimeException layer per call.
 */
public class EnsureTransactionTests extends HGTestBase
{
    static final IllegalStateException UNCHECKED = new IllegalStateException("unchecked");

    static Callable<Object> throwing(final Exception ex)
    {
        return new Callable<Object>() {
            public Object call() throws Exception { throw ex; }
        };
    }

    @Test
    public void uncheckedExceptionUnchangedWithoutTransaction()
    {
        try
        {
            graph.getTransactionManager().ensureTransaction(throwing(UNCHECKED));
            fail("expected an exception");
        }
        catch (IllegalStateException ex)
        {
            assertSame(UNCHECKED, ex);
        }
    }

    @Test
    public void uncheckedExceptionUnchangedInNestedCalls()
    {
        final HGTransactionManager tm = graph.getTransactionManager();
        try
        {
            tm.transact(new Callable<Object>() {
                public Object call()
                {
                    // two levels of ensureTransaction inside an active transaction
                    return tm.ensureTransaction(new Callable<Object>() {
                        public Object call() { return tm.ensureTransaction(throwing(UNCHECKED)); }
                    });
                }
            });
            fail("expected an exception");
        }
        catch (IllegalStateException ex)
        {
            assertSame("not wrapped", UNCHECKED, ex);
        }
    }

    @Test
    public void checkedExceptionWrappedInHGException()
    {
        final HGTransactionManager tm = graph.getTransactionManager();
        final IOException checked = new IOException("checked");
        try
        {
            tm.transact(new Callable<Object>() {
                public Object call() { return tm.ensureTransaction(throwing(checked)); }
            });
            fail("expected an exception");
        }
        catch (HGException ex)
        {
            assertSame(checked, ex.getCause());
        }
    }
}
