package hgtest.tx;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import hgtest.HGTestBase;

public class NoTxTests extends HGTestBase
{
    @BeforeClass
    public void setUp()
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
}
