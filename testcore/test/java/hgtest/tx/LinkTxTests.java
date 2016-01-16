package hgtest.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import hgtest.HGTestBase;
import hgtest.T;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.HGUserAbortException;
import org.hypergraphdb.util.HGUtils;
import org.junit.Test;

public class LinkTxTests extends HGTestBase
{
    private int atomsCount = 40; // must be an even number
    private int linksCount = 20; // must be an even number
    private int threadCount = 4;
    private boolean log = !true;
    
    private ArrayList<Throwable> errors = new ArrayList<Throwable>();
    
    public static class LinkType extends HGPlainLink
    {
        private int threadId = -1;
        private int idx = -1;
        
        public LinkType(HGHandle...targets)
        {
            super(targets);
        }
        
        public LinkType(int threadId, int idx, HGHandle...targets)
        {
            super(targets);
            this.threadId = threadId;
            this.idx = idx;
        }
        
        public int getThreadId()
        {
            return threadId;
        }

        public void setThreadId(int threadId)
        {
            this.threadId = threadId;
        }

        public int getIdx()
        {
            return idx;
        }

        public void setIdx(int idx)
        {
            this.idx = idx;
        }               
    }
    
    Object makeAtom(int i)
    {
        return "linkAtom" + i;
    }
    
    private void checkLinks(final HGHandle h1, HGHandle h2)
    {
        int j = 0;
        do
        {
            LinkType x = hg.getOne(graph, hg.and(hg.type(LinkType.class), 
                                                 hg.eq("idx", j), 
                                                 hg.incident(h1),
                                                 hg.incident(h2)));
            assertNotNull(x);
            ++j;
            LinkType y = hg.getOne(graph, hg.and(hg.type(LinkType.class), 
                                                 hg.eq("idx", j), 
                                                 hg.incident(h1),
                                                 hg.incident(h2)));
            assertNotNull(y);
            assertEquals(x.getThreadId(), y.getThreadId());
            ++j;
        } while (j < linksCount);        
    }
    
    public void checkLinkSanity(HGPersistentHandle handle)
    {
        HGPersistentHandle [] atomLayout = graph.getStore().getLink(handle);
        HGPersistentHandle [] valueLayout = graph.getStore().getLink(atomLayout[1]);
        if (HGUtils.eq(atomLayout, valueLayout))
            System.out.println("problem");
    }
    
    public void checkLinkSanity()
    {
        int ok = 0, ko = 0;
        HGSearchResult<HGPersistentHandle> rs = graph.find(hg.type(LinkType.class));
        while (rs.hasNext())
        {
            HGPersistentHandle [] atomLayout = graph.getStore().getLink(rs.next());
            HGPersistentHandle [] valueLayout = graph.getStore().getLink(atomLayout[1]);
            if (HGUtils.eq(atomLayout, valueLayout))
            {
                System.err.println("messy.");
                ko++;
            }
            ok++;
        }
        rs.close();
        System.out.println("ok="+ok + ", ko=" + ko);
    }
    
    public void verifyData()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            HGHandle h1 = hg.findOne(graph, hg.eq(makeAtom(i)));
            assertNotNull(h1);
            assertEquals(graph.getIncidenceSet(h1).size(), linksCount*(atomsCount - 1));
            for (int j = i + 1; j < atomsCount; j++)
            {
                HGHandle h2 = hg.findOne(graph, hg.eq(makeAtom(j)));
                assertNotNull(h2);                
                checkLinks(h1, h2);
            }
        }
    }
    
    private void linkThem(final int threadId, final HGHandle x, final HGHandle y)
    {
        int i = 0;
        final HGTransactionManager txman = graph.getTransactionManager();
        while (i < linksCount - 1)
        {
            final int finali = i;
            Integer result = txman.transact(new Callable<Integer>() {
            public Integer call()
            {
                LinkType first = new LinkType(threadId, finali, x, y);
                HGHandle existing = hg.findOne(graph, hg.and(hg.type(LinkType.class), 
                                                             hg.eq("idx", finali), 
                                                             hg.incident(x), 
                                                             hg.incident(y)));
                if (existing != null)
                    return finali + 1;
                checkLinkSanity(graph.getPersistentHandle(graph.add(first)));
                int next = finali + 1;
                existing = hg.findOne(graph, hg.and(hg.type(LinkType.class), 
                                                    hg.eq("idx", next), 
                                                    hg.incident(x), 
                                                    hg.incident(y)));
                if (existing != null)
                {
                    if (log)
                        T.getLogger("LinkTxTests").info("Aborting because of " + finali + "-" + next + " at " + threadId
                                + ", x=" + x + ", y=" + y);
                    throw new HGUserAbortException();
                }
                else
                {
                    if (log)
                        T.getLogger("LinkTxTests").info("Fine with " + finali + "-" + next + " at " + threadId);                    
                    checkLinkSanity(graph.getPersistentHandle(graph.add(new LinkType(threadId, next, x , y))));
                }
                return finali + 2;
            }
            });
            if (result == null) i++;
            else i = result;
            try { Thread.sleep((long)Math.random()*100); }
            catch (InterruptedException ex) {}
        }
    }
    
    private void populateLinks(int threadId)
    {
        for (int i = 0; i < atomsCount; i++)
        {            
            HGHandle hi = hg.findOne(graph, hg.eq(makeAtom(i)));
            assertNotNull(hi);
            for (int j = i + 1; j < atomsCount; j++)
            {
                HGHandle hj = hg.findOne(graph, hg.eq(makeAtom(j)));
                assertNotNull(hj);
                if (log)
                    T.getLogger("LinkTxTests").info("Linking " + i + " <-> " + j);
                System.out.println("Linking " + i + " <-> " + j);
                linkThem(threadId, hi, hj);
//                ((WeakRefAtomCache)graph.getCache()).printSizes();
            }
        }        
    }

    @Test
    public void testConcurrentLinkCreation()
    {
        graph.getIndexManager().register(new ByPartIndexer(graph.getTypeSystem().getTypeHandle(LinkType.class), "idx"));
        graph.runMaintenance();
        for (int i = 0; i < atomsCount; i++)
            hg.assertAtom(graph, makeAtom(i));        
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < threadCount; i++)
        {
            final int j = i; 
            pool.execute(new Runnable() {
                public void run()
                {
                    try
                    {
                        populateLinks(j);
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace(System.err);
                        errors.add(t);
                    }
                }
            });
        }
        try
        {
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex) 
        {
            System.out.println("testTxMap interrupted.");
            return;
        }        
        assertEquals(errors.size(), 0);
        this.reopenDb();
        verifyData();
    }
    
    public static void main(String [] argv)
    {        
        LinkTxTests test = new LinkTxTests();
//        test.graph = HGEnvironment.get(test.getGraphLocation());
//        test.checkLinkSanity();
        
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.testConcurrentLinkCreation();
        }
        finally
        {
            test.tearDown();
        }        
    }
}