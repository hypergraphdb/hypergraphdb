package hgtest.indexing;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.SimpleBean;

public class IndexManagerTests extends HGTestBase
{
    @Test
    public void testUnregister()
    {
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
}