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

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.util.HGUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTxTests extends HGTestBase
{
    private int atomsCount = 200;
    private int threadCount = 2;

    private boolean log = false;

    private ArrayList<Throwable> errors = new ArrayList<Throwable>();

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

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + idx;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SimpleData other = (SimpleData) obj;
            if (idx != other.idx)
                return false;
            if (value == null)
            {
                if (other.value != null)
                    return false;
            }
            else if (!value.equals(other.value))
                return false;
            return true;
        }        
        
        
    }

    Object makeAtom(int i)
    {
        return new SimpleData(i, i);
    }

    public void verifyData()
    {
        for (int i = 0; i < atomsCount; i++)
        {
            SimpleData x = hg.getOne(graph, hg.and(hg.type(SimpleData.class),
                    hg.eq("idx", i)));
            if (log && x.getIdx() == 0)
                T.getLogger("DataTxTests").info("verifyData: " + x);
            assertNotNull(x);
            assertEquals(x.getValue(), x.getIdx() + threadCount);
        }
    }

    private ThreadLocal<HashMap<Integer, SimpleData>> localMap =
        new ThreadLocal<HashMap<Integer, SimpleData>>();
    
    private void increment(final HGPersistentHandle atomX)
    {        
        final HGTransactionManager txman = graph.getTransactionManager();
        SimpleData committed = txman.ensureTransaction(new Callable<SimpleData>() 
        { 
            public SimpleData call()
            {
                final SimpleData l = graph.get(atomX);
                if (log && l.getIdx() == 0)
                    T.getLogger("DataTxTests").info(
                            "Increment " + l + ":" + atomX);            
                SimpleData newBean = new SimpleData(l.getIdx(), l.getValue() + 1);
                graph.replace(atomX, newBean);
                if (log && l.getIdx() == 0)
                    T.getLogger("DataTxTests").info("After increment " + l);
                return newBean;
            }
        });
        localMap.get().put(committed.getIdx(), committed);        
    }

    private void incrementValues()
    {
        localMap.set(new HashMap<Integer, SimpleData>());        
        for (int i = 0; i < atomsCount; i++)
            localMap.get().put(i, (SimpleData)hg.getOne(graph, hg.and(hg.type(SimpleData.class),
                    hg.eq("idx", i))));                    
        for (int i = 0; i < atomsCount; i++)
        {
            final HGPersistentHandle hi = hg.findOne(graph, hg.and(hg.type(SimpleData.class),
                                                         hg.eq("idx", i)));
            assertNotNull(hi);            
            increment(hi);                
        }
    }

    @Test
    public void testConcurrentAtomLoad()
    {
    	final HGPersistentHandle handle = graph.add("testConcurrentAtomLoad").getPersistent();
    	int nthreads = 100;
    	this.reopenDb();
    	ExecutorService pool = Executors.newFixedThreadPool(nthreads);
    	final ArrayList<Object> failedIfNotEmpty = new ArrayList<Object>();
    	for (int i = 0; i < nthreads; i++)
    		pool.submit(new Runnable(){
    			public void run()
    			{
    				if (graph.getHandle(graph.get(handle)) == null)
    					failedIfNotEmpty.add(Boolean.TRUE);
    			}
    		});
    	pool.shutdown();
    	try
		{
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	Assert.assertEquals(failedIfNotEmpty.size(), 0);
    }
    
    @Test
    public void testConcurrentLinkCreation()
    {
        graph.getStore().getIndex("temptest", BAtoBA.getInstance(), BAtoBA.getInstance(), null, true);
        for (int i = 0; i < atomsCount; i++)
            hg.assertAtom(graph, makeAtom(i));
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
        for (int i = 0; i  < Integer.MAX_VALUE; i++ ){
        DataTxTests test = new DataTxTests();
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.testConcurrentLinkCreation();
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
            break;
        }
        finally
        {
            test.tearDown();
        }
        }
    }
}