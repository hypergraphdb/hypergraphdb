package hgtest.benchmark;

import static org.testng.Assert.*;
import hgtest.HGTestBase;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.Test;

public class SimpleTx extends HGTestBase
{
    private int count = 20000;
    private int poolSize = 10;
    
    public void checkRange(int start, int end)
    {
        for (int i = start; i < end; i++)
        {
            
            String current = "currrent" + i;
            HGHandle h = hg.findOne(graph, hg.eq(current));
            assertNotNull(h);
            assertNotNull(hg.and(hg.eq("rel"), hg.incident(h), hg.arity(0)));
        }
    }
    
    public void doRange(int start, int end)
    {
//        graph.getTransactionManager().beginTransaction();
        long begin = System.currentTimeMillis();
        for (int i = start; i < end; i++)
        {
            
            String current = "currrent" + i;
            HGHandle h = graph.add(current);            
            HGValueLink link = new HGValueLink("rel", h);
            graph.add(link);
            
            if (i % 10 == 0) 
            {
//                graph.getTransactionManager().commit();
//                graph.getTransactionManager().beginTransaction();
                if (i % 1000 == 0)                    
                {
                    System.out.println("Time at " + start + ":" + i + " -- " + (System.currentTimeMillis()-begin)/1000);
//                    try { Thread.sleep(5000); }
//                    catch (InterruptedException ex) { }                    
                }
            }            
        }        
//        graph.getTransactionManager().commit();        
    }
    
    @Test
    public void oneAtomOneLink()
    {
        long start = System.currentTimeMillis();
//        graph.getTransactionManager().beginTransaction();
        for (int i = 0; i < count; i++)
        {
            
            String current = "currrent" + i;
            HGHandle h = graph.add(current);            
            HGValueLink link = new HGValueLink("rel", h);
            graph.add(link);
            
            if (i % 10 == 0) 
            {
//                graph.getTransactionManager().commit();
//                graph.getTransactionManager().beginTransaction();
                if (i % 1000 == 0)
                    System.out.println("Time at " + i + " -- " + (System.currentTimeMillis()-start)/1000);
            }            
        }
//        graph.getTransactionManager().commit();
        System.out.println("Time=" + (System.currentTimeMillis()-start));
    }

    @Test
    public void parallelBulkAdd()
    {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        assertEquals(count % poolSize, 0);
        final int batchSize = count / poolSize;
        for (int i = 0; i < poolSize; i++)
        {
            final int j = i; 
            pool.execute(new Runnable() {
                public void run()
                {
                    doRange(j*batchSize, j*batchSize + batchSize);
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
            
        }
    }
 
    public static void main(String [] argv)
    {
        SimpleTx test = new SimpleTx();
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
//            test.oneAtomOneLink();
            test.parallelBulkAdd();
            test.checkRange(0, 200);
        }
        finally
        {
            test.tearDown();
        }
    }    
}
