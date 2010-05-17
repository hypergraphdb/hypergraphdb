package hgtest.tx;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hypergraphdb.HGHandle;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.SimpleBean;

public class TypingTxTests extends HGTestBase
{
    @Test
    public void testTypeAbort()
    {
        HGHandle h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        graph.getTransactionManager().beginTransaction();
        h = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        assertNotNull(h, null);
        graph.getTransactionManager().abort();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        reopenDb();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);        
    }
    
    @Test(invocationCount=1)
    public void testConcurrentTypeAdd()
    {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Callable<HGHandle> op = new Callable<HGHandle>()
        {
            public HGHandle call()
            {
                return graph.getTypeSystem().getTypeHandle(SimpleBean.class);
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