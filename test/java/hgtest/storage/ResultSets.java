package hgtest.storage;

import hgtest.HGTestBase;


import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.util.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ResultSets extends HGTestBase
{
    private static final String ALIAS_PREFIX = "TestIntAlias";
    static String HG_PATH = "c:/temp/graphs/test";
    final static int COUNT = 10;
    final static int ALIAS_COUNT = 5;
    HGSortIndex<Integer, HGHandle> index;

    public static void main(String[] args)
    {
        new ResultSets().test();
    }

    public void test()
    {
        setUp();
        testAlGenerator();
        testSingleValueResultSet();
        testKeyScanResultSet();
        testKeyRangeForwardResultSet();
        testKeyRangeBackwardResultSet();
        testFilteredResultSet();
        testSingleKeyResultSet();
        testUnionResult();
        testInMemoryIntersectionResult();
        tearDown();
       
    }

    @BeforeClass
    public void setUp()
    {
        super.setUp();
        HGTypeSystem ts = graph.getTypeSystem();
        HGHandle typeH = ts.getTypeHandle(TestInt.class);
        for (int i = 0; i < ALIAS_COUNT; i++)
            ts.addAlias(typeH, ALIAS_PREFIX + i);

        index = (HGSortIndex<Integer, HGHandle>)
            graph.getIndexManager().<Integer, HGHandle>register(new ByPartIndexer(typeH, "x"));
        for (int i = 0; i < COUNT; i++)
            graph.add(new TestInt(i));
    }

    @AfterClass
    public void tearDown()
    {
        List<HGHandle> list = hg.findAll(graph, hg.type(TestInt.class));
        for (HGHandle handle : list)
            graph.remove(handle);
        HGTypeSystem ts = graph.getTypeSystem();
        for (int i = 0; i < ALIAS_COUNT; i++)
            try
            {
                ts.removeAlias(ALIAS_PREFIX + i);
            }
            catch (Throwable t)
            {
            }
        List<HGIndexer> indexers = graph.getIndexManager().getIndexersForType(
                graph.getTypeSystem().getTypeHandle(TestInt.class));
        if (indexers != null)
            for (HGIndexer indexer : indexers)
                graph.getIndexManager().deleteIndex(indexer);
        super.tearDown();
    }

    @Test
    public void testUnionResult()
    {
        HGSearchResult<HGHandle> res = 
            graph.find(
                    hg.and(hg.type(TestInt.class),
                    hg.or(hg.eq(new TestInt(9)), 
                            hg.eq(new TestInt(5)))));
        try
        {
            Assert.assertTrue(expectedType(res, "UnionResult"));
            List<Integer> list = result__list(graph, res);
            Assert.assertEquals(list.size(), 2);
            List<Integer> back_list = back_result__list(graph, res);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
    }
    
    @Test
    public void testInMemoryIntersectionResult()
    {
//        HGHandle h = graph.add(new TestLink.Int(1000));
//        HGHandle linkH = graph.add(new TestLink(h));
//       // graph.add(new TestLink.Int(-1000));
//        HGQuery q = 
//                HGQuery.make(graph,
//                        hg.orderedLink(h, HGHandleFactory.anyHandle,
//                                HGHandleFactory.anyHandle));
//        HGSearchResult<HGHandle> res = 
//            q.execute();
//      try
//      {
//          //Assert.assertTrue(expectedType(res, "InMemoryIntersectionResult"));
//          List<Integer> list = result__list(graph, res);
//          Assert.assertEquals(list.size(), 2);
//          List<Integer> back_list = back_result__list(graph, res);
//          Assert.assertTrue(reverseLists(list, back_list));
//      }
//      finally
//      {
//          res.close();
//      }
    }
    
    @Test
    public void testAlGenerator()
    {
        HGHandle needH = graph.add(new TestLink.Int(1000));
        HGHandle anotherH = graph.add(new TestLink.Int(-1000));
        graph.add(new TestLink(needH));
        graph.add(new HGPlainLink(needH, anotherH));
        DefaultALGenerator gen = new DefaultALGenerator(graph, null, null);
        HGSearchResult<Pair<HGHandle, HGHandle>> i =  gen.generate(needH);
        while (i.hasNext())
        {
            Assert.assertNotNull(i.next().getFirst());
        }
        
    }
    
    @Test
    public void testSingleValueResultSet()
    {
        HGBidirectionalIndex<String, HGPersistentHandle> idx = graph.getStore()
                .getBidirectionalIndex(
                /* HGTypeSystem.TYPE_ALIASES_DB_NAME, */
                "hg_typesystem_type_alias", BAtoString.getInstance(),
                        BAtoHandle.getInstance(), null);
        HGSearchResult<String> res = idx.findByValue(graph
                .getPersistentHandle(graph.getTypeSystem().getTypeHandle(
                        TestInt.class)));
        try
        {
            Assert.assertTrue(expectedType(res, "SingleValueResultSet"));
            List<String> list = new ArrayList<String>();
            while (res.hasNext())
                list.add(res.next());
            Assert.assertEquals(list.size(), ALIAS_COUNT);
            List<String> back_list = new ArrayList<String>();
            back_list.add(res.current());
            while (res.hasPrev())
                back_list.add(res.prev());
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
            checkBeforeFirstAfterLastNotEmptyRS((HGRandomAccessResult) res);
        }
        finally
        {
            res.close();
        }
    }

    @Test
    public void testKeyRangeForwardResultSet()
    {
        HGSearchResult<HGHandle> res = index.findGTE(5);
        try
        {
            //boolean b = res.hasPrev();
            Assert.assertTrue(expectedType(res, "KeyRangeForwardResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertTrue(isSortedList(list, true));
            Assert.assertEquals(list.size(), 5);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
            checkBeforeFirstAfterLastNotEmptyRS((HGRandomAccessResult) res);
        }
        finally
        {
            res.close();
        }
        bounds_test(index.findGTE(-1), false);
    }

    @Test
    public void testKeyRangeBackwardResultSet()
    {
        HGSearchResult<HGHandle> res = index.findLT(5);
        try
        {
            Assert.assertTrue(expectedType(res, "KeyRangeBackwardResult"));
            List<Integer> list = result__list(graph, res);
            Assert.assertTrue(isSortedList(list, false));
            Assert.assertEquals(list.size(), 5);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
            checkBeforeFirstAfterLastNotEmptyRS((HGRandomAccessResult) res);
        }
        finally
        {
            res.close();
        }        
        bounds_test(index.findLT(10), true);
    }
    
    @Test
    public void testFilteredResultSet()
    {
        Assert.assertEquals(hg.findAll(graph, hg.type(TestInt.class)).size(), 10);
        HGQueryCondition cond = hg.lte(new TestInt(5)); 
            // hg.and(hg.type(TestInt.class), hg.lte("x", 5));
        HGQuery<HGHandle> q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        try
        {
//            Assert.assertTrue(expectedType(res, "FilteredResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertEquals(list.size(), COUNT - 5 + 1);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
        
        //cond = hg.and(hg.type(TestInt.class), hg.lte(new TestInt(10)));
        res = graph.find(hg.lte(new TestInt(10)));
        bounds_test(res, true);       
        //cond = hg.and(hg.type(TestInt.class), hg.gte(new TestInt(-1)));
        res = graph.find(hg.gte(new TestInt(-1)));
        bounds_test(res, false);
    }
    
    void bounds_test(HGSearchResult<HGHandle> res, boolean upper)
    {
        try
        {
            Assert.assertTrue(!res.hasPrev());
            Assert.assertTrue(res.hasNext());
            print(result__list(graph, res)); 
        }
        finally
        {
            res.close();
        }
    }

    @Test
    public void testSingleKeyResultSet()
    {
        HGQueryCondition cond = hg.and(hg.type(TestInt.class));
        HGQuery<HGHandle> q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        try
        {
            Assert.assertTrue(expectedType(res, "SingleKeyResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertEquals(list.size(), COUNT);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
            checkBeforeFirstAfterLastNotEmptyRS((HGRandomAccessResult) res);
        }
        finally
        {
            res.close();
        }
    }

    @Test
    public void testKeyScanResultSet()
    {
        HGSearchResult<Integer> res = index.scanKeys();
        try
        {
            Assert.assertTrue(expectedType(res, "KeyScanResultSet"));
            List<Integer> list = new ArrayList<Integer>();
            while (res.hasNext())
                list.add(res.next());
            Assert.assertEquals(list.size(), COUNT);
            List<Integer> back_list = new ArrayList<Integer>();
            back_list.add(res.current());
            while (res.hasPrev())
                back_list.add(res.prev());
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
            checkBeforeFirstAfterLastNotEmptyRS((HGRandomAccessResult) res);
        }
        finally
        {
            res.close();
        }
    }
    
    private void checkBeforeFirstAfterLastNotEmptyRS(HGRandomAccessResult res)
    {
        res.goAfterLast();
        Assert.assertFalse(res.hasNext());
        Assert.assertNotNull(res.prev());
        res.goBeforeFirst();
        Assert.assertFalse(res.hasPrev());
        Assert.assertNotNull(res.next());
    }

    static List<Integer> result__list(HyperGraph graph,
            HGSearchResult<HGHandle> res)
    {
        List<Integer> list = new ArrayList<Integer>();
        while (res.hasNext())
            list.add(((TestInt) graph.get(res.next())).getX());
        return list;
    }

    static boolean expectedType(Object o, String t)
    {
        return o.getClass().getName().indexOf(t) > -1;
    }

    static boolean isSortedList(List<Integer> list, boolean up)
    {
        if (list.isEmpty())
            return true;
        int curr = list.get(0);
        for (int i = 1; i < list.size(); i++)
        {
            if (up && curr < list.get(i) || !up && curr > list.get(i))
                curr = list.get(i);
            else
                return false;
        }
        return true;
    }

    static boolean reverseLists(List<?> list, List<?> other)
    {
        if (list.size() != other.size())
            return false;
        int size = list.size();
        for (int i = 0; i < list.size(); i++)
            if (!list.get(i).equals(other.get(size - 1 - i)))
                return false;
        return true;
    }

    static List<Integer> back_result__list(HyperGraph graph,
            HGSearchResult<HGHandle> res)
    {
        List<Integer> list = new ArrayList<Integer>();
        // add the last result which is current right now
        Integer o = ((TestInt) graph.get(res.current())).getX();
        list.add(o);
        while (res.hasPrev())
            list.add(((TestInt) graph.get(res.prev())).getX());
        return list;
    }

    static void print(HyperGraph graph, HGSearchResult<HGHandle> res)
    {
        // System.out.println("HGSearchResult: " + res);
        List<Integer> list = result__list(graph, res);
        print(list);
    }

    static void print(List<?> list)
    {
        System.out.println("Reversed list");
        for (int i = 0; i < list.size(); i++)
            System.out.println(":" + i + ":" + list.get(i));
    }

}
