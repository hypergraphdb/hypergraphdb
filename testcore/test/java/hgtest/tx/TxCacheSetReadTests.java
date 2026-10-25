package hgtest.tx;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TxCacheSet;
import org.hypergraphdb.transaction.TxSet.SetTxBox;
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.HGSortedSet;
import org.hypergraphdb.util.RefCountedMap;
import org.hypergraphdb.util.RefResolver;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * A TxCacheSet created inside a transaction that is older than the most recent commit gets a
 * "reload, a newer version may exist" marker on top of its versions. Reading it outside of any
 * transaction must load the latest state instead of returning the marker's null (which made queries
 * with incident(...) conditions fail with a NullPointerException).
 */
public class TxCacheSetReadTests extends HGTestBase
{
    static HGSortedSet<String> set(String... values)
    {
        return new ArrayBasedSet<String>(values);
    }

    /** Commit a write transaction on another thread, so that the current transaction becomes an older one. */
    static void commitNewerTransaction() throws Exception
    {
        ExecutorService other = Executors.newSingleThreadExecutor();
        try
        {
            other.submit(() -> graph.getTransactionManager().transact(new Callable<Object>() {
                public Object call() { return graph.add("newer transaction " + System.nanoTime()); }
            })).get(30, TimeUnit.SECONDS);
        }
        finally
        {
            other.shutdownNow();
        }
    }

    /** Create a TxCacheSet inside a transaction that is older than the most recent commit. */
    static TxCacheSet<String, String> cachedInOlderTransaction(RefResolver<String, HGSortedSet<String>> loader) throws Exception
    {
        HGTransactionManager tm = graph.getTransactionManager();
        tm.beginTransaction();
        try
        {
            commitNewerTransaction();
            return new TxCacheSet<String, String>(tm, set("a", "b", "c"), "key",
                                                  loader, new RefCountedMap<String, SetTxBox<String>>(null));
        }
        finally
        {
            tm.endTransaction(true);
        }
    }

    @Test
    public void readOutsideTransactionAfterCachingInOlderTransaction() throws Exception
    {
        final AtomicInteger loads = new AtomicInteger();
        TxCacheSet<String, String> cached = cachedInOlderTransaction(new RefResolver<String, HGSortedSet<String>>() {
            public HGSortedSet<String> resolve(String key)
            {
                loads.incrementAndGet();
                return set("a", "b", "c");
            }
        });

        // Outside any transaction:
        assertEquals(3, cached.size());
        assertTrue(cached.contains("b"));
        HGRandomAccessResult<String> rs = cached.getSearchResult();
        try
        {
            assertTrue(rs.hasNext());
            assertEquals("a", rs.next());
        }
        finally
        {
            rs.close();
        }
        assertEquals("loaded once, then kept as the latest version", 1, loads.get());
    }

    /**
     * A commit that lands while the set is being reloaded: the loaded state is returned but not
     * kept as the latest version (it may be newer than the version number taken before loading),
     * so the next read reloads. The reload doesn't hold the commit lock, so the commit can proceed.
     */
    @Test
    public void commitDuringReloadIsNotCached() throws Exception
    {
        final AtomicInteger loads = new AtomicInteger();
        TxCacheSet<String, String> cached = cachedInOlderTransaction(new RefResolver<String, HGSortedSet<String>>() {
            public HGSortedSet<String> resolve(String key)
            {
                if (loads.incrementAndGet() == 1)
                    try
                    {
                        commitNewerTransaction();
                    }
                    catch (Exception ex)
                    {
                        throw new RuntimeException(ex);
                    }
                return set("a", "b", "c");
            }
        });

        assertEquals(3, cached.size());   // reload, a commit lands meanwhile: not cached
        assertEquals(1, loads.get());
        assertEquals(3, cached.size());   // reloads, no commit meanwhile: cached
        assertEquals(2, loads.get());
        assertEquals(3, cached.size());   // served from the cache
        assertEquals(2, loads.get());
    }

    /**
     * The reload above reads storage outside of any transaction and relies on such reads seeing
     * committed data only: a non-transactional read must never return another transaction's
     * uncommitted change (it may wait for it, or fail with a lock conflict).
     */
    @Test
    public void nonTransactionalStorageReadSeesOnlyCommittedData() throws Exception
    {
        final HGHandle atom = graph.add("isolation test atom " + System.nanoTime());
        final CountDownLatch linkAdded = new CountDownLatch(1), readDone = new CountDownLatch(1);
        final AtomicReference<HGPersistentHandle> uncommittedLink = new AtomicReference<HGPersistentHandle>();
        ExecutorService writer = Executors.newSingleThreadExecutor();
        Future<?> writing = writer.submit(() -> {
            HGTransactionManager tm = graph.getTransactionManager();
            tm.beginTransaction();
            try
            {
                uncommittedLink.set(graph.add(new HGPlainLink(atom)).getPersistent());
                linkAdded.countDown();
                readDone.await(30, TimeUnit.SECONDS);
            }
            finally
            {
                tm.endTransaction(false);   // abort: the link never existed
            }
            return null;
        });
        try
        {
            assertTrue(linkAdded.await(30, TimeUnit.SECONDS));
            assertNull("no transaction on the reading thread", graph.getTransactionManager().getContext().getCurrent());
            List<HGPersistentHandle> seen = new ArrayList<HGPersistentHandle>();
            try
            {
                HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(atom.getPersistent());
                try
                {
                    while (rs.hasNext())
                        seen.add(rs.next());
                }
                finally
                {
                    rs.close();
                }
            }
            catch (RuntimeException lockConflict)
            {
                // Waiting for the writer's lock and timing out is acceptable; seeing its change is not.
            }
            assertFalse("uncommitted link must not be visible: " + seen, seen.contains(uncommittedLink.get()));
        }
        finally
        {
            readDone.countDown();
            writing.get(30, TimeUnit.SECONDS);
            writer.shutdownNow();
        }
    }
}
