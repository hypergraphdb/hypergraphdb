package hgtest.tx;

import java.util.concurrent.Callable;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import hgtest.HGTestBase;

public class NestedTxTests extends HGTestBase
{
	// need to ignore because BDB Java Edition does not support nested transactions, 
	// unlike the native, should have a configuration/capability setting somewhere for 
	// this as it's not a commonly needed feature
	@Test
	@Ignore 
	public void testISChange()
	{
		final HyperGraph graph = getGraph();
		graph.getTransactionManager().transact(new Callable<Object>() {
			public Object call()
			{
				final HGHandle myatom = graph.add("testISChange");
				// That'll bring the incidence set into the cache
				Assert.assertTrue(graph.getIncidenceSet(myatom).isEmpty());
				graph.getTransactionManager().transact(new Callable<Object>(){
					public Object call()
					{
						// modify a structure in the cache within a nested transaction
						graph.add(new HGPlainLink(myatom));
						return null;
					}
				});				
				return null;
			}
		});
		HGHandle x = hg.findOne(graph, hg.eq("testISChange"));
		Assert.assertNotNull(x);
		// the incidence set, still in the cache, should be updated
		Assert.assertEquals(graph.getIncidenceSet(x).size(), 1);
	}
}