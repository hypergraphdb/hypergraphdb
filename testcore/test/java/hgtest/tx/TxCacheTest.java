package hgtest.tx;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import hgtest.HGTestBase;
import hgtest.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGAtomAttrib;
import org.hypergraphdb.HGAtomCache;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.cache.WeakRefAtomCache;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TxMap;
import org.hypergraphdb.util.CloneMe;

public class TxCacheTest extends HGTestBase
{
    private int atomsCount = 10000;
    private int threadCount = 20;

    private boolean log = false;

    private ArrayList<Throwable> errors = new ArrayList<Throwable>();
    private HGAtomCache cache = null;
    private TxMap<HGPersistentHandle, Integer> data = null;
    private Map<Integer, HGPersistentHandle> handleMap = new HashMap<Integer, HGPersistentHandle>();
    
    public static class SimpleData implements CloneMe
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
        
        public SimpleData duplicate()
        {
            return new SimpleData(idx, value.get());
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
            Integer x = data.get(handleMap.get(i));
            if (log)
                T.getLogger("TxCacheTest").info("verifyData: " + x);
            assertNotNull(x);
            assertEquals(x.intValue(), i + threadCount);
        }
    }
  
    private void increment(final int idx)
    {        
        final HGTransactionManager txman = graph.getTransactionManager();
        txman.transact(new Callable<SimpleData>() 
        { 
            public SimpleData call()
            {
                HGPersistentHandle handle = handleMap.get(idx);
                HGLiveHandle lHandle = cache.get(handle);
                SimpleData x = null;
                if (lHandle != null)
                    x = (SimpleData)lHandle.getRef();
                if (x == null)
                {
                    Integer value = data.get(handle);
                    x = new SimpleData(idx, value);
                    lHandle = cache.atomRead(handle, x, new HGAtomAttrib());
                }
                SimpleData newBean = x.duplicate();
                newBean.incrementValue();                
                cache.atomRefresh(lHandle, newBean, true);
                data.put(handle, newBean.getValue());
                return newBean;
            }
        });
    }

    private void incrementValues()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            increment(i);
        }
    }

    // @Test
    public void runMe()
    {
        cache = new WeakRefAtomCache();
        cache.setHyperGraph(graph);
        data = new TxMap<HGPersistentHandle, Integer>(graph.getTransactionManager(), null);
        handleMap.clear();
        for (int i = 0; i < atomsCount; i++)
        {
            graph.getTransactionManager().beginTransaction();
            final int fi = i;
            HGPersistentHandle handle = graph.getHandleFactory().makeHandle();
            handleMap.put(fi, handle);
            cache.atomAdded(handle, new SimpleData(i, i), null);
            data.put(handle, fi);
            try { graph.getTransactionManager().endTransaction(true); }
            catch (Exception ex) { throw new RuntimeException(ex); }
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
        TxCacheTest test = new TxCacheTest();
        test.setUp();        
        try
        {
            for (int i = 0; i < 100; i++)
            {
                test.runMe();
//                System.out.println("" + i + "th done, CONFLICTS="
//                        + test.graph.getTransactionManager().conflicted.get()
//                        + ", SUCCESSFUL="
//                        + test.graph.getTransactionManager().successful.get());
            }
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