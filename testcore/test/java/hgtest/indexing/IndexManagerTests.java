package hgtest.indexing;

import org.hypergraphdb.HGIndex;

import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.type.TypeUtils;
import org.junit.Assert;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.beans.SimpleBean;

public class IndexManagerTests extends HGTestBase
{
	private void cleanup()
	{
		TypeUtils.deleteType(graph, graph.getTypeSystem().getTypeHandle(SimpleBean.class));
	}
	
    @Test
    public void testUnregister()
    {
    	cleanup();
        HGIndexer indexer = new ByPartIndexer(graph.getTypeSystem().getTypeHandle(SimpleBean.class), "strProp");
        graph.getIndexManager().register(indexer);
        graph.runMaintenance();
        SimpleBean x = new SimpleBean();
        x.setStrProp("hypergraphdb");
        graph.add(x);
        x = new SimpleBean();
        x.setStrProp("indexing");
        graph.add(x);
        HGIndex<?, ?> idx = graph.getIndexManager().getIndex(indexer);
        Assert.assertNotNull(idx);
        Assert.assertEquals(idx.count(), 2);
        HGIndexer indexer2 = new ByPartIndexer(graph.getTypeSystem().getTypeHandle(SimpleBean.class), "strProp");
        Assert.assertTrue(graph.getIndexManager().unregister(indexer2));
        Assert.assertNull(graph.getIndexManager().getIndex(indexer));
        this.reopenDb();
        Assert.assertNull(graph.getIndexManager().getIndex(indexer));
        Assert.assertEquals(graph.getIndexManager().getIndexersForType(graph.getTypeSystem().getTypeHandle(SimpleBean.class)), 
                null);
    }
    
    @Test
    public void testNewIndexWithExistingData()
    {
    	cleanup();
        SimpleBean x = new SimpleBean();
        x.setStrProp("hypergraphdb");
        graph.add(x);
    	HGIndexer indexer = new ByPartIndexer(graph.getTypeSystem().getTypeHandle(SimpleBean.class), "strProp");
        HGIndex<?, ?> theIndex = graph.getIndexManager().register(indexer);
        x = new SimpleBean();
        x.setStrProp("indexing");
        graph.add(x);    	        
        // The index should be empty here
        Assert.assertEquals(theIndex.count(), 0);
    }
    
    @Test
    public void testNewIndexWithoutExistingData()
    {
    	cleanup();
    	HGIndexer indexer = new ByPartIndexer(graph.getTypeSystem().getTypeHandle(SimpleBean.class), "strProp");
        HGIndex<?, ?> theIndex = graph.getIndexManager().register(indexer);
        SimpleBean x = new SimpleBean();
        x.setStrProp("indexing");
        graph.add(x);    	        
        // The index should contain exactly one element here:
        Assert.assertEquals(theIndex.count(), 1);	
    }
}