package hgtest.utils;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TxMap;
import org.hypergraphdb.transaction.VBox;
import org.hypergraphdb.util.WeakIdentityHashMap;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import hgtest.HGTestBase;

public class TestTxMap extends HGTestBase
{
    @Test
    public void testWeakIdentityMap()
    {
        WeakIdentityHashMap<Integer, Boolean> map = new WeakIdentityHashMap<Integer, Boolean>();
        long start = System.currentTimeMillis();
        long time = 1000*60*10; // 10 minutes
        while (true)
        {
            map.put((int)(Math.random()*Integer.MAX_VALUE), Boolean.TRUE);
            if (System.currentTimeMillis() - start > time)
                break;
        }
    }
    
    private void mapPopulate(final int agentId, final TxMap<Integer, Integer> txMap, final int max)
    {
        int i = 0;
        final HGTransactionManager txman = graph.getTransactionManager();
        while (i < max)
        {
            final int x = i;
            txman.transact(new Callable<Object>() {
            public Object call()
            {
                if (txMap.containsKey(x))
                    return null;
                txMap.put(x, agentId);
                int y = x + 1;
                if (txMap.containsKey(y))
                    txman.abort();
                else
                    txMap.put(y, agentId);
                return null;
            }
            });
            i++;
            try { Thread.sleep((long)Math.random()*100); }
            catch (InterruptedException ex) {}
        }
    }
    
    private void checkMap(final TxMap<Integer, Integer> txMap, final int max)
    {
        HashMap<Integer, Integer> M = new HashMap<Integer, Integer>();
        for (int i = 0; i < max; i += 2)
        {
            int curr = txMap.get(i);
            int next = txMap.get(i + 1);
            assertEquals(curr, next);
            
            Integer cnt = M.get(curr);
            if (cnt == null)
                M.put(curr, 2);
            else
                M.put(curr, cnt+2);
        }
        System.out.println(M.toString());
    }    
    
    @Test
    public void testTxMap()
    {
        final int agents = 10;
        final int max = 1000*100;
        
        final TxMap<Integer, Integer> txMap = new TxMap<Integer, Integer>(graph.getTransactionManager(),
                                                    new HashMap<Integer, VBox<Integer>>());
        
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < agents; i++)
        {
            final int j = i; 
            pool.execute(new Runnable() {
                public void run()
                {
                    mapPopulate(j, txMap, max);
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
        assertEquals(txMap.size(), max);        
        checkMap(txMap, max);       

        for (int i = 0; i < max; i++)
        {
            graph.getTransactionManager().beginTransaction();
            txMap.remove(i);
            try
            {
                graph.getTransactionManager().endTransaction(true);
            }
            catch (Throwable t) { t.printStackTrace(System.err); }
        }
        
        assertEquals(txMap.size(), 0);
        assertEquals(txMap.mapSize(), 0);             
    }
    
    
    @Test
    public void testTxMapAbort()
    {
        //
        // This test commits max / 2 entries in the map and aborts another max / 2. The end result
        // is that the map size should be max / 2, and the map.mapSize (its internal map size) should
        // also be max / 2, making sure that aborted transaction don't leave entries erroneously hanging
        // in the map.
        //
        
        final int max = 1000*100;
        
        final TxMap<Integer, Integer> txMap = new TxMap<Integer, Integer>(graph.getTransactionManager(),
                                                    new HashMap<Integer, VBox<Integer>>());
        try
        {
            for (int i = 0; i < max; i++)
            {
                graph.getTransactionManager().beginTransaction();
                txMap.put(i, i);
                if (i % 2 == 0)
                    graph.getTransactionManager().commit();
                else
                    graph.getTransactionManager().abort();
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
        
        assertEquals(txMap.size(), max / 2);
        assertEquals(txMap.mapSize(), txMap.size());
    }
    
}
