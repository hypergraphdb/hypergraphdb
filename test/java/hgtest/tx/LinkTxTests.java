package hgtest.tx;

import java.util.ArrayList;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import hgtest.HGTestBase;
import hgtest.T;

public class LinkTxTests extends HGTestBase
{
    private int atomsCount = 100; // must be an even number
    private int linksCount = 50; // must be an even number
    private int threadCount = 5;
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
            i = txman.transact(new Callable<Integer>() {
            public Integer call()
            {
                LinkType first = new LinkType(threadId, finali, x, y);
                HGHandle existing = hg.findOne(graph, hg.and(hg.type(LinkType.class), 
                                                             hg.eq("idx", finali), 
                                                             hg.incident(x), 
                                                             hg.incident(y)));
                if (existing != null)
                    return finali + 1;
                
                graph.add(first);
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
                    txman.abort();
                }
                else
                {
                    if (log)
                        T.getLogger("LinkTxTests").info("Fine with " + finali + "-" + next + " at " + threadId);                    
                    graph.add(new LinkType(threadId, next, x , y));
                }
                return finali + 2;
            }
            });
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
            }
        }        
    }

    @Test
    public void testConcurrentLinkCreation()
    {
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
        verifyData();
    }
    
    public static void main(String [] argv)
    {
        LinkTxTests test = new LinkTxTests();
        dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.graph.getTransactionManager().conflicted.set(0);
            test.graph.getTransactionManager().successful.set(0);
            test.testConcurrentLinkCreation();
            System.out.println("Done, CONFLICTS=" + test.graph.getTransactionManager().conflicted.get() +
                               ", SUCCESSFUL=" + test.graph.getTransactionManager().successful.get());
        }
        finally
        {
            test.tearDown();
        }        
    }
}