package hgtest.tx;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import hgtest.HGTestBase;

public class NoTxTests extends HGTestBase
{
    @BeforeClass
    public static void setUp()
    {
        HGUtils.dropHyperGraphInstance(getGraphLocation());
        HGConfiguration config = new HGConfiguration();
        config.setTransactional(false);
        graph = HGEnvironment.get(getGraphLocation(), config);        
    }

    @Test
    public void testAtomAdd()
    {
    	graph.add("Hello");
    	Assert.assertEquals("Hello", hg.getOne(graph, hg.eq("Hello")));
    	reopenDb();
    	Assert.assertEquals("Hello", hg.getOne(graph, hg.eq("Hello")));
    }
    
    @Test
    public void testAddRemove()
    {
    	for (int i = 0; i < 1000; i++)
    	{
    		graph.add(Math.random());
    	}
    	Assert.assertEquals(hg.count(graph, hg.type(Double.class)), 1000);
    	List<Double> L = hg.getAll(graph, hg.type(Double.class));
    	int removed = 0;
    	for (Double d : L)
    		if (d < 0.5)
    		{
    			graph.remove(graph.getHandle(d));
    			removed++;
    		}
    	reopenDb();
    	Assert.assertEquals(hg.count(graph, hg.type(Double.class)), 1000 - removed);
    }
}
