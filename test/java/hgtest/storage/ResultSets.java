package hgtest.storage;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.storage.DefaultIndexImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResultSets
{
    private static final String ALIAS_PREFIX = "TestIntAlias";
    static String HG_PATH = "c:/temp/graphs/test";
    final static int COUNT = 10;
    final static int ALIAS_COUNT = 5;
    HyperGraph graph;
    DefaultIndexImpl index;

    public static void main(String[] args)
    {
        new ResultSets().test();
    }

    @Test
    public void test()
    {
        graph = HGEnvironment.get(HG_PATH);
        clean();
        prepare();
        testSingleValueResultSet();
        testKeyScanResultSet();
        testKeyRangeForwardResultSet();
        testKeyRangeBackwardResultSet();
        testFilteredResultSet();
        testSingleKeyResultSet();

        graph.close();
    }

    void prepare()
    {
        HGTypeSystem ts = graph.getTypeSystem();
        HGHandle typeH = ts.getTypeHandle(TestInt.class);
        for (int i = 0; i < ALIAS_COUNT; i++)
            ts.addAlias(typeH, ALIAS_PREFIX + i);

        index = (DefaultIndexImpl) graph.getIndexManager().register(
                new ByPartIndexer(typeH, "x"));
        for (int i = 0; i < COUNT; i++)
            graph.add(new TestInt(i));
    }

    void clean()
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
        if (indexers != null) for (HGIndexer indexer : indexers)
            graph.getIndexManager().deleteIndex(indexer);
    }

    // SingleValueResultSet
    void testSingleValueResultSet()
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
            //print(list);  print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
    }

    // KeyRangeForwardResultSet
    void testKeyRangeForwardResultSet()
    {
        HGSearchResult<HGHandle> res = index.findGTE(5);
        try
        {
            boolean b = res.hasPrev();
            Assert.assertTrue(expectedType(res, "KeyRangeForwardResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertTrue(isSortedList(list, true));
            Assert.assertEquals(list.size(), 5);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
    }

    // KeyRangeBackwardResult
    void testKeyRangeBackwardResultSet()
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
        }
        finally
        {
            res.close();
        }
    }

    // FilteredResultSet
    void testFilteredResultSet()
    {
        HGQueryCondition cond = hg.and(hg.type(TestInt.class), hg
                .lte(new TestInt(5)));
        HGQuery q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        try
        {
            Assert.assertTrue(expectedType(res, "FilteredResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertEquals(list.size(), COUNT);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
    }

    // SingleKeyResultSet
    void testSingleKeyResultSet()
    {
        HGQueryCondition cond = hg.and(hg.type(TestInt.class));
        HGQuery q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        try
        {
            Assert.assertTrue(expectedType(res, "SingleKeyResultSet"));
            List<Integer> list = result__list(graph, res);
            Assert.assertEquals(list.size(), COUNT);
            List<Integer> back_list = back_result__list(graph, res);
            // print(list); print(back_list);
            Assert.assertTrue(reverseLists(list, back_list));
        }
        finally
        {
            res.close();
        }
    }

    // KeyScanResultSet
    void testKeyScanResultSet()
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
        }
        finally
        {
            res.close();
        }
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
        if (list.isEmpty()) return true;
        int curr = list.get(0);
        for (int i = 1; i < list.size(); i++)
        {
            if (up && curr < list.get(i) || !up && curr > list.get(i)) curr = list
                    .get(i);
            else
                return false;
        }
        return true;
    }

    static boolean reverseLists(List<?> list, List<?> other)
    {
        if (list.size() != other.size()) return false;
        int size = list.size();
        for (int i = 0; i < list.size(); i++)
            if (!list.get(i).equals(other.get(size - 1 - i))) return false;
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
