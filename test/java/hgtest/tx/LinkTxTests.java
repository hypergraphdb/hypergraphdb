package hgtest.tx;

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

public class LinkTxTests extends HGTestBase
{
    private int atomsCount = 100;
    private int linksCount = 1000;
    private int threadCount = 10;
    
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
    
    public void verifyData()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            HGHandle h = hg.findOne(graph, hg.eq(makeAtom(i)));
            assertNotNull(h);
            assertEquals(graph.getIncidenceSet(h).size(), linksCount);
            int j = 0;
            do
            {
                LinkType x = hg.getOne(graph, hg.and(hg.type(LinkType.class), hg.eq("idx", j), hg.incident(h)));
                assertNotNull(x);
                ++j;
                LinkType y = hg.getOne(graph, hg.and(hg.type(LinkType.class), hg.eq("idx", j), hg.incident(h)));
                assertNotNull(y);
                assertEquals(x.getThreadId(), y.getThreadId());
                ++j;
            } while (j < linksCount);
        }
    }
    
    private void linkThem(final int threadId, final HGHandle x, final HGHandle y)
    {
        int i = 0;
        final HGTransactionManager txman = graph.getTransactionManager();
        while (i < linksCount)
        {
            final int finali = i;
            txman.transact(new Callable<Object>() {
            public Object call()
            {
                LinkType first = new LinkType(threadId, finali, x, y);
                HGHandle existing = hg.findOne(graph, hg.and(hg.type(LinkType.class), 
                                                             hg.eq("idx", finali), 
                                                             hg.incident(x), 
                                                             hg.incident(y)));
                if (existing != null)
                    return null;
                graph.add(first);
                int next = finali + 1;
                existing = hg.findOne(graph, hg.and(hg.type(LinkType.class), 
                                                    hg.eq("idx", next), 
                                                    hg.incident(x), 
                                                    hg.incident(y)));
                if (existing != null)
                    txman.abort();
                else
                    graph.add(new LinkType(threadId, next, x , y));
                return null;
            }
            });
            i++;
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
                linkThem(threadId, hi, hj);
            }
        }        
    }
    
    private int fact(int n) { return n == 0 ? 1 : n*fact(n-1); }
    
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
                    populateLinks(j);
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
        
        assertEquals(hg.count(graph, hg.type(LinkType.class)), 
                     linksCount*fact(atomsCount)/fact(2)/fact(atomsCount-2));
        
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