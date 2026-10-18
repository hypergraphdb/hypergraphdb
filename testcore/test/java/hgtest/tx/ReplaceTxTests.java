package hgtest.tx;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.handle.HGLiveHandle;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * Replacing an atom with a different runtime instance, and the live handles that point to it.
 * The old live handle must keep pointing to the old instance until the replacing transaction
 * commits: after an abort the cache still holds the original runtime instance (same Java object),
 * and transactions other than the replacing one keep seeing it until the commit.
 */
public class ReplaceTxTests extends HGTestBase
{
    private static ExecutorService otherThread;

    public static class Holder
    {
        private String value;

        public Holder() { }
        public Holder(String value) { this.value = value; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public String toString() { return "Holder(" + value + ")"; }
    }

    @BeforeClass
    public static void startOtherThread()
    {
        otherThread = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void stopOtherThread()
    {
        otherThread.shutdownNow();
    }

    private HGHandle addHolder(String value)
    {
        HGHandle h = graph.add(new Holder(value));
        assertTrue("precondition: add returns a live handle", h instanceof HGLiveHandle);
        return h;
    }

    private Object getFromOtherThread(final HGHandle h) throws Exception
    {
        return otherThread.submit(() -> graph.get(h)).get(30, TimeUnit.SECONDS);
    }

    @Test
    public void replaceAbortKeepsRuntimeInstance()
    {
        HGHandle h = addHolder("original");
        Holder original = graph.get(h);
        graph.getTransactionManager().beginTransaction();
        graph.replace(h, new Holder("replacement"));
        graph.getTransactionManager().abort();
        assertSame("same Java object through the live handle", original, graph.get(h));
        assertSame("same Java object through the persistent handle", original, graph.get(h.getPersistent()));
        assertEquals("original", original.getValue());
        graph.remove(h);
    }

    @Test
    public void replaceCommitRedirectsOldLiveHandle()
    {
        HGHandle h = addHolder("original");
        Holder replacement = new Holder("replacement");
        graph.getTransactionManager().beginTransaction();
        graph.replace(h, replacement);
        graph.getTransactionManager().commit();
        assertSame("old live handle resolves to the new instance", replacement, graph.get(h));
        assertSame(replacement, graph.get(h.getPersistent()));
        assertSame(replacement, graph.get(graph.refreshHandle(h)));
        graph.remove(h);
    }

    @Test
    public void replacingTransactionSeesNewInstanceThroughOldHandle()
    {
        HGHandle h = addHolder("original");
        Holder replacement = new Holder("replacement");
        graph.getTransactionManager().beginTransaction();
        try
        {
            graph.replace(h, replacement);
            assertSame("old live handle, inside the replacing transaction", replacement, graph.get(h));
            assertSame(replacement, graph.get(h.getPersistent()));
            assertSame("refreshHandle inside the replacing transaction", replacement,
                       graph.get(graph.refreshHandle(h)));
            assertSame("freeze through the old live handle", replacement, graph.freeze(h));
            graph.unfreeze(h);
        }
        finally
        {
            graph.getTransactionManager().commit();
        }
        graph.remove(h);
    }

    @Test
    public void otherTransactionsSeeOriginalUntilCommit() throws Exception
    {
        HGHandle h = addHolder("original");
        Holder original = graph.get(h);
        Holder replacement = new Holder("replacement");
        graph.getTransactionManager().beginTransaction();
        try
        {
            graph.replace(h, replacement);
            assertSame("another thread, before commit", original, getFromOtherThread(h));
        }
        finally
        {
            graph.getTransactionManager().commit();
        }
        assertSame("another thread, after commit", replacement, getFromOtherThread(h));
        graph.remove(h);
    }

    @Test
    public void severalReplacesThroughOldHandleThenCommit()
    {
        HGHandle h = addHolder("original");
        Holder first = new Holder("first");
        Holder second = new Holder("second");
        graph.getTransactionManager().beginTransaction();
        graph.replace(h, first);
        graph.replace(h, second);
        graph.getTransactionManager().commit();
        assertSame(second, graph.get(h));
        assertNull("no cache entry left for the intermediate instance", graph.getHandle(first));
        graph.remove(h);
    }

    @Test
    public void abortAfterSeveralReplacesKeepsRuntimeInstance()
    {
        HGHandle h = addHolder("original");
        Holder original = graph.get(h);
        graph.getTransactionManager().beginTransaction();
        graph.replace(h, new Holder("first"));
        graph.replace(h, new Holder("second"));
        assertEquals("second", ((Holder)graph.get(h)).getValue());
        graph.getTransactionManager().abort();
        assertSame(original, graph.get(h));
        assertEquals("original", original.getValue());
        graph.remove(h);
    }
}
