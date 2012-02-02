package hgtest.tx;

import static org.testng.Assert.assertEquals;


import static org.testng.Assert.assertNotNull;
import hgtest.HGTestBase;
import hgtest.T;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TxCacheMap;

public class TxCacheMapTest extends HGTestBase
{
    private int atomsCount = 10000;
    private int threadCount = 20;

    private boolean log = false;

    private ArrayList<Throwable> errors = new ArrayList<Throwable>();
    private TxCacheMap<Integer, SimpleData> theMap = null;
    
    public static class SimpleData
    {
        private int idx = -1;
        private AtomicInteger value = new AtomicInteger(0);

        public SimpleData()
        {
        }

        public SimpleData(int idx, int val)
        {
            this.idx = idx;
            this.value.set(val);
        }

        @Override
        public String toString()
        {
            return "SimpleData [idx=" + idx
                    + ", value=" + value + "]";
        }

        public int getIdx()
        {
            return idx;
        }

        public void setIdx(int idx)
        {
            this.idx = idx;
        }

        public int getValue()
        {
            return value.get();
        }

        public void setValue(int value)
        {
            this.value.set(value);
        }
        
        public void incrementValue()
        {
            this.value.incrementAndGet();
        }
        
        public void decrementValue()
        {
            this.value.incrementAndGet();
        }        
    }

    SimpleData makeAtom(int i)
    {
        return new SimpleData(i, i);
    }

    public void verifyData()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            SimpleData x = theMap.get(i);
            if (log && x.getIdx() == 0)
                T.getLogger("DataTxTests").info("verifyData: " + x);
            assertNotNull(x);
            assertEquals(x.getValue(), x.getIdx() + threadCount);
        }
    }

    private ThreadLocal<HashMap<Integer, SimpleData>> localMap =
        new ThreadLocal<HashMap<Integer, SimpleData>>();

    //private TxMap<Integer, SimpleData> localMap = null;
    
    private void increment(final int idx)
    {        
        final HGTransactionManager txman = graph.getTransactionManager();
        SimpleData committed = txman.transact(new Callable<SimpleData>() 
        { 
            public SimpleData call()
            {
                SimpleData l = theMap.get(idx);
                if (l == null)
                {
                    l = localMap.get().get(idx);
                    System.out.println("loading old...");                   
                    theMap.load(idx, l);
                }
                if (log && l.getIdx() == 0)
                    T.getLogger("DataTxTests").info(
                            "Increment " + l + ":" + idx);            
                SimpleData newBean = new SimpleData(l.getIdx(), l.getValue() + 1);
                theMap.put(idx, newBean);                
                if (log && l.getIdx() == 0)
                    T.getLogger("DataTxTests").info("After increment " + l);
                return newBean;
            }
        });
        localMap.get().put(idx, committed);
        //System.out.println("Committed : " + committed + " from " + Thread.currentThread().getId());
    }

    private void incrementValues()
    {
        localMap.set(new HashMap<Integer, SimpleData>());        
        for (int i = 0; i < atomsCount; i++)
            localMap.get().put(i, theMap.get(i));
        for (int i = 0; i < atomsCount; i++)
        {
            increment(i);
        }
    }

    // @Test
    public void runMe()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            final int fi = i; 
            graph.getTransactionManager().transact(new Callable<Object>() {public Object call() {
            theMap.put(fi, makeAtom(fi));
            return null;}});
        }
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < threadCount; i++)
        {
            pool.execute(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        incrementValues();
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

    public static void main(String[] argv)
    {
        TxCacheMapTest test = new TxCacheMapTest();
        test.setUp();        
        try
        {
            test.theMap = new TxCacheMap<Integer, SimpleData>(test.graph.getTransactionManager(), HashMap.class, null);
            //test.localMap = new TxMap<Integer, SimpleData>(test.graph.getTransactionManager(), null);
            test.runMe();
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
            test.tearDown();
        }
    }
}