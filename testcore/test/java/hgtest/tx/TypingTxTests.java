package hgtest.tx;

import java.util.concurrent.Callable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;

import static org.junit.Assert.*;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.beans.SimpleBean;

public class TypingTxTests extends HGTestBase
{
    @Test
    public void testTypeAbort()
    {
        HGHandle h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        graph.getTransactionManager().beginTransaction();
        h = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        assertNotNull(h);
        graph.getTransactionManager().abort();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        reopenDb();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);        
    }
    
    @Test
    public void testConcurrentTypeAdd()
    {
    	for (int i = 0; i <100; i++)
    	{
	        ExecutorService pool = Executors.newFixedThreadPool(2);
	        Callable<HGHandle> op = new Callable<HGHandle>()
	        {
	            public HGHandle call()
	            {
	                HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
	                double x = Math.random();
	                SimpleBean bean = new SimpleBean();
	                bean.setDoubleProp(x);
	                HGHandle h = graph.add(bean);
	                assertEquals(h, hg.findOne(graph, hg.and(hg.type(SimpleBean.class), hg.eq("doubleProp", x))));
	                return typeHandle;
	            }
	        };
	        Future<HGHandle> f1 = pool.submit(op);
	        Future<HGHandle> f2 = pool.submit(op);
	        try
	        {
	            assertEquals(f1.get(), f2.get());
	        }
	        catch (InterruptedException e)
	        {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        catch (ExecutionException e)
	        {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        finally
	        {
	            try
	            {
	                graph.remove(f1.get());
	                graph.remove(f2.get());
	            }
	            catch (Throwable e)
	            {
	                e.printStackTrace();
	            }
	        }
    	}
    }
    
    @Test
    public void testConcurrentTypeRemove()
    {
    	for (int i = 0; i < 100; i++)
    	{
	        long totalAtoms = hg.count(graph, hg.all());
	        final HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
	        graph.add(new SimpleBean());
	        graph.add(new SimpleBean());
	        ExecutorService pool = Executors.newFixedThreadPool(2);
	        Callable<HGHandle> op = new Callable<HGHandle>()
	        {
	            public HGHandle call()
	            {
	                graph.remove(typeHandle);
	                return typeHandle;
	            }
	        };
	        try
	        {
	            Future<HGHandle> f1 = pool.submit(op);
	            Future<HGHandle> f2 = pool.submit(op);        
	            f1.get();
	            f2.get();
	        }
	        catch (InterruptedException e)
	        {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        catch (ExecutionException e)
	        {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        assertNull(graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class));
	        assertEquals(totalAtoms, hg.count(graph, hg.all()));
    	}
   }
    
    public static void main(String[] argv)
    {
        TypingTxTests test = new TypingTxTests();
        try
        {
            test.setUp();
            test.testTypeAbort();
            test.testConcurrentTypeAdd();
            test.testConcurrentTypeRemove();
            System.out.println("test passed successfully");
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