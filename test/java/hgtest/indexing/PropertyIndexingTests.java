package hgtest.indexing;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.query.impl.IndexBasedQuery;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.SimpleBean;
import hgtest.beans.DerivedBean;

public class PropertyIndexingTests extends HGTestBase
{
    @SuppressWarnings("unchecked")
    @Test
    void simplePropertyIndexing()
    {
        HGHandle simpleTypeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        HGIndex<?,?> idx = graph.getIndexManager().register(new ByPartIndexer(simpleTypeHandle, "intProp"));
        
        for (int i = 0; i < 100; i++)
        {
            SimpleBean bean = new SimpleBean();
            bean.setIntProp(i);
            graph.add(bean);
        }

        try
        {
            Assert.assertEquals(idx.count(), 100);
            reopenDb();
            idx = graph.getIndexManager().getIndex(new ByPartIndexer(simpleTypeHandle, "intProp"));
            Assert.assertEquals(idx.count(), 100);
    
            // check that an index will be used if querying by that property:
            ExpressionBasedQuery<HGHandle> query = 
                (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 2)));
            Assert.assertTrue(query.getCompiledQuery() instanceof IndexBasedQuery, "Compiled query using index.");
        }
        finally
        {
            // cleanup
            List<HGHandle> L = hg.findAll(graph, hg.type(SimpleBean.class)); 
            for (HGHandle x : L)
                graph.remove(x);
        }   
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void derivedPropertyIndexing()
    {
        HGHandle simpleTypeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        HGHandle derivedTypeHandle = graph.getTypeSystem().getTypeHandle(DerivedBean.class);
        HGIndex<?,?> idx = graph.getIndexManager().register(new ByPartIndexer(simpleTypeHandle, "intProp"));
        HGIndex<?,?> idx2 = graph.getIndexManager().register(new ByPartIndexer(derivedTypeHandle, "derivedProperty"));

        for (int i = 0; i < 50; i++)
        {
            DerivedBean bean = new DerivedBean();
            bean.setIntProp(i);
            bean.setDerivedProperty(Double.toString(Math.random()));
            graph.add(bean);
        }
        
        Assert.assertEquals(idx.count(), 50);
        Assert.assertEquals(idx2.count(), 50);
        
        reopenDb();
        
        for (int i = 50; i < 100; i++)
        {
            DerivedBean bean = new DerivedBean();
            bean.setIntProp(i);
            bean.setDerivedProperty(Double.toString(Math.random()));
            graph.add(bean);
        }
        
        System.out.println("SimpleBean type " + graph.getTypeSystem().getTypeHandle(SimpleBean.class));
        System.out.println("DerivedBean type " + graph.getTypeSystem().getTypeHandle(DerivedBean.class));

        idx = graph.getIndexManager().getIndex(new ByPartIndexer(simpleTypeHandle, "intProp"));
        idx2 = graph.getIndexManager().getIndex(new ByPartIndexer(derivedTypeHandle, "derivedProperty"));

        try
        {
            Assert.assertEquals(idx.count(), 100);
            Assert.assertEquals(idx2.count(), 100);
            reopenDb();
            idx = graph.getIndexManager().getIndex(new ByPartIndexer(simpleTypeHandle, "intProp"));
            idx2 = graph.getIndexManager().getIndex(new ByPartIndexer(derivedTypeHandle, "derivedProperty"));
            Assert.assertEquals(idx.count(), 100);
            Assert.assertEquals(idx2.count(), 100);
            
            ExpressionBasedQuery<HGHandle> query = 
                (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 2)));
            Assert.assertTrue(query.getCompiledQuery() instanceof IndexBasedQuery, "Compiled query using index.");
    
            query = (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(DerivedBean.class), hg.eq("intProp", 2)));
            Assert.assertTrue(query.getCompiledQuery() instanceof IntersectionQuery, "Compiled query using index of base type for derived type.");
            IntersectionQuery compiled = (IntersectionQuery)query.getCompiledQuery();
            Assert.assertTrue(compiled.getLeft() instanceof IndexBasedQuery || compiled.getRight() instanceof IndexBasedQuery, "Compiled query using index of base type for derived type.");
            
            query = 
                (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(DerivedBean.class), hg.eq("derivedProperty", "243.234")));
            Assert.assertTrue(query.getCompiledQuery() instanceof IndexBasedQuery, "Compiled query using index.");
            
        }
        finally
        {
            List<HGHandle> L = hg.findAll(graph, hg.type(DerivedBean.class));
            for (HGHandle x : L)
                graph.remove(x);
        }
    }    
}