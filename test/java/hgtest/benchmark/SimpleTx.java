package hgtest.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import hgtest.HGTestBase;
import hgtest.PrivateConstructible;

import org.hypergraphdb.*;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.testng.annotations.Test;

public class SimpleTx extends HGTestBase
{
    
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
        for (int i = 0; i < 2*200000; i++)
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
        final int batchSize = 200000;
        for (int i = 0; i < 10; i++)
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
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex) 
        {
            
        }
    }
 
    public static void main(String [] argv)
    {
        SimpleTx test = new SimpleTx();
        dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            //test.oneAtomOneLink();
            test.parallelBulkAdd();
        }
        finally
        {
            test.tearDown();
        }
    }    
}
