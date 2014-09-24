package hgtest.indexing;

import java.util.ArrayList;
import java.util.List;


import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.DirectValueIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.query.impl.IndexBasedQuery;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.RandomStringUtils;
import hgtest.T;
import hgtest.DebugTest.Id;
import hgtest.beans.SimpleBean;
import hgtest.beans.DerivedBean;

public class PropertyIndexingTests extends HGTestBase
{
    @Test
    public void testUpdateLiveAtom()
    {
        HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        ByPartIndexer<Long> byPartIndexer = new ByPartIndexer<Long>("id_indexer", typeHandle, "longProp");
        graph.getIndexManager().register(byPartIndexer);
        graph.runMaintenance();
        SimpleBean x = new SimpleBean();
        x.setLongProp(1l);
        graph.add(x);
        x.setLongProp(3l);
        graph.update(x);
        Assert.assertEquals(graph.count(hg.and(hg.type(SimpleBean.class), hg.gt("longProp", 0l))), 1);
    }
    
    @Test
    public void testNumberOrder()
    {
        HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        ByPartIndexer<Long> byPartIndexer = new ByPartIndexer<Long>("id_indexer", typeHandle, "longProp");
        graph.getIndexManager().register(byPartIndexer);

        for (long i = 1l; i < 2000l; i++)
        {
            graph.add(new Id(i));
        }
        
        HGIndex index = graph.getIndexManager().getIndex(byPartIndexer);
        HGRandomAccessResult result = index.scanValues();
        result.goBeforeFirst();
        try
        {
            ArrayList<Long> numbers = new ArrayList<Long>();
            while (result.hasNext())
            {
                SimpleBean b = graph.get((HGHandle) result.next());
                numbers.add(b.getLongProp());
            }
            for (int i = 0; i < numbers.size() - 1; i++)
                Assert.assertTrue(numbers.get(i) < numbers.get(i+1));
        }
        finally
        {
            result.close();
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void simplePropertyIndexing()
    {
        HGHandle simpleTypeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        HGIndex<?,?> idx = graph.getIndexManager().register(new ByPartIndexer(simpleTypeHandle, "intProp"));
        HGIndex<?,?> sidx = graph.getIndexManager().register(new ByPartIndexer(simpleTypeHandle, "strProp"));
        for (HGIndexer I : graph.getIndexManager().getIndexersForType(simpleTypeHandle))
        	System.out.println("Comp: " + I.getComparator(graph));
        int totalAdded = 0;
        ArrayList<String> strings = new ArrayList<String>();
        for (int i = 0; i < 100; i++)
        {
            SimpleBean bean = new SimpleBean();
            bean.setIntProp(i);
            
            // The random string property of the SimpleBean is also added
            // a few times as a separate atom to bump up the string reference
            // counting. This tests the String comparator that must be in effect
            // in the index by 'strProp'. 
            String sprop = RandomStringUtils.random(10 + T.random(10));
            int cnt = T.random(20);
            for (int j = 0; j < cnt; j++)
            {
            	graph.add(sprop);
            }
        	strings.add(sprop);            
           	bean.setStrProp(sprop);                 
            graph.add(bean);
            totalAdded++;
        }

        try
        {
            Assert.assertEquals(idx.count(), totalAdded);
            reopenDb();
            idx = graph.getIndexManager().getIndex(new ByPartIndexer(simpleTypeHandle, "intProp"));
            Assert.assertEquals(idx.count(), totalAdded);
    
            // check that an index will be used if querying by that property:
            ExpressionBasedQuery<HGHandle> query = 
                (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 56)));
            Assert.assertTrue(query.getCompiledQuery() instanceof IndexBasedQuery, "Compiled query using index.");
            Assert.assertEquals(hg.count(query), 1);
            for (String s : strings)
            	Assert.assertNotNull(hg.findOne(graph, hg.and(hg.type(SimpleBean.class), hg.eq("strProp", s))));
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
    public void derivedPropertyIndexing()
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
            System.out.println("Compiled query type:" + query.getCompiledQuery().getClass());
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
    
    @Test
    public void valueLinkByPropertyTest()
    {
        HGHandle simpleTypeHandle = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        HGIndex<?,?> idx = graph.getIndexManager().register(new ByPartIndexer(simpleTypeHandle, "intProp"));
        HGHandle last = simpleTypeHandle;
        int totalAdded = 0;
        for (int i = 0; i < 100; i++)
        {
            SimpleBean bean = new SimpleBean();
            bean.setIntProp(i);
            last = graph.add(new HGValueLink(bean, new HGHandle[]{ last }));
            totalAdded++;
        }        
        try
        {
            Assert.assertEquals(idx.count(), totalAdded);
            reopenDb();
            idx = graph.getIndexManager().getIndex(new ByPartIndexer(simpleTypeHandle, "intProp"));
            Assert.assertEquals(idx.count(), totalAdded);    
            // check that an index will be used if querying by that property:
            ExpressionBasedQuery<HGHandle> query = 
                (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 56)));
            Assert.assertTrue(query.getCompiledQuery() instanceof IndexBasedQuery, "Compiled query using index.");
            Assert.assertEquals(hg.count(query), 1);
            
            HGHandle h1 = graph.findOne(hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 56)));
            Assert.assertNotNull(h1);
            List<HGHandle> lessthen = graph.findAll(hg.and(hg.type(SimpleBean.class), 
                    hg.lt("intProp", 200))); 
            List<HGHandle> incident = graph.findAll(hg.and(hg.type(SimpleBean.class), 
                    hg.incident(h1)));
            System.out.println(lessthen.size());
            System.out.println(lessthen);
            System.out.println(incident.size());
            System.out.println(incident);
            System.out.println(lessthen.contains(incident.iterator().next()));
            query = (ExpressionBasedQuery)HGQuery.make(graph, hg.and(hg.type(SimpleBean.class), 
                    hg.lt("intProp", 200), hg.incident(h1)));
            Assert.assertEquals(query.findAll().size(), 1);
        }
        finally
        {
            // cleanup
            List<HGHandle> L = hg.findAll(graph, hg.type(SimpleBean.class)); 
            for (HGHandle x : L)
                graph.remove(x);
        }   
    }
}