package hgtest.tx;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.type.JavaTypeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.beans.SimpleBean;
import org.junit.Assert;

// Those tests fail with BerkeleyDB Java Edition
// which doesn't have proper snapshot isolation.
public class ConcurrentTypeCreation extends HGTestBase
{
	ExecutorService pool;
	
	@Before
	public void createExecutionPool()
	{
		pool =  Executors.newFixedThreadPool(10);
	}
	
	@After
	public void destroyExecutionPool()
	{
		pool.shutdownNow();
	}
	
	@Test
	public void concurrentSlotCreation()
	{
		final String label = "title";
		final HGHandle stringType = graph.getTypeSystem().getTypeHandle(String.class);
		for (int i = 0; i < 50; i++)
		{
			try
			{
				Future<HGHandle> f1 = pool.submit(() -> {
					return graph.getTransactionManager().ensureTransaction(() -> {
						System.out.println("1 : get handle");
						return JavaTypeFactory.getSlotHandle(graph, label, stringType);
					});
				});
				Future<HGHandle> f2 = pool.submit(() -> {
					return graph.getTransactionManager().ensureTransaction(() -> {
						System.out.println("2 : get handle");
						return JavaTypeFactory.getSlotHandle(graph, label, stringType);
					});
				});			
				Assert.assertEquals(f1.get(), f2.get());
			}
			catch (Throwable t)
			{
				t.printStackTrace(System.err);
				throw new RuntimeException(t);
			}
			HGTestBase.tearDown();
			HGTestBase.setUp();
			System.out.println("Parallel slot creation iteration: " + i);
		}
	}

	
	@Test
	public void concurrentTypeCreation()
	{
		for (int i = 0; i < 50; i++)
		{
			try
			{
				Future<?> f1 = pool.submit(() -> {
					graph.add(new SimpleBean());
				});
				Future<?> f2 = pool.submit(() -> {
					graph.add(new SimpleBean());
				});			
				f2.get();
				f1.get();
			}
			catch (Throwable t)
			{
				t.printStackTrace(System.err);
				throw new RuntimeException(t);
			}
			HGTestBase.tearDown();
			HGTestBase.setUp();
		}
	}
}
