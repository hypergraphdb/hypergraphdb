package hgtest.storage;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.DefaultIndexImpl;
import org.testng.Assert;


public class ResultSets
{
    static String HG_PATH = "c:/temp/graphs/test";
    
    final static int COUNT = 10;
    
    public static void main(String[] args)
    {
        HyperGraph graph = HGEnvironment.get(HG_PATH);
        clean(graph);
        HGHandle th = graph.getTypeSystem().getTypeHandle(TestInt.class);
        DefaultIndexImpl index = (DefaultIndexImpl)
          graph.getIndexManager().register(new ByPartIndexer(th, "x"));
        for (int i = 0; i < COUNT; i++)
           graph.add(new TestInt(i));
               
        testKeyRangeForwardResultSet(graph, index);
        testKeyRangeBackwardResultSet(graph, index);
        testFilteredResultSet(graph, index);
        testSingleKeyResultSet(graph, index);
               
        graph.close();
    }
    
    //KeyRangeForwardResultSet
    static void testKeyRangeForwardResultSet(HyperGraph graph, DefaultIndexImpl index)
    {
        HGSearchResult<HGHandle> res = index.findGTE(5);
        Assert.assertTrue(expectedType(res, "KeyRangeForwardResultSet"));
        List<Integer> list = result__list(graph, res);
        Assert.assertTrue(isSortedList(list, true));
        Assert.assertEquals(list.size(), 5);
        List<Integer> back_list = back_result__list(graph, res);
        //print(list); print(back_list);
        Assert.assertTrue(reverseLists(list, back_list));
    }
    
    //KeyRangeBackwardResult
    static void testKeyRangeBackwardResultSet(HyperGraph graph, DefaultIndexImpl index)
    {
        HGSearchResult<HGHandle> res = index.findLT(5);
        Assert.assertTrue(expectedType(res, "KeyRangeBackwardResult"));
        List<Integer> list = result__list(graph, res);
        Assert.assertTrue(isSortedList(list, false));
        Assert.assertEquals(list.size(), 5);
        List<Integer> back_list = back_result__list(graph, res);
       //print(list); print(back_list);
        Assert.assertTrue(reverseLists(list, back_list));
    }
    
    //FilteredResultSet
    static void testFilteredResultSet(HyperGraph graph, DefaultIndexImpl index)
    {
        HGQueryCondition cond = hg.and(hg.type(TestInt.class), hg.lte(new TestInt(5)));
        HGQuery q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        Assert.assertTrue(expectedType(res, "FilteredResultSet"));
        List<Integer> list = result__list(graph, res);
        Assert.assertEquals(list.size(), COUNT);
        List<Integer> back_list = back_result__list(graph, res);
        //print(list); print(back_list);
        Assert.assertTrue(reverseLists(list, back_list));
    }
    
    //SingleKeyResultSet
    static void testSingleKeyResultSet(HyperGraph graph, DefaultIndexImpl index)
    {
        HGQueryCondition cond = hg.and(hg.type(TestInt.class));
        HGQuery q = HGQuery.make(graph, cond);
        HGSearchResult<HGHandle> res = q.execute();
        Assert.assertTrue(expectedType(res, "SingleKeyResultSet"));
        List<Integer> list = result__list(graph, res);
        Assert.assertEquals(list.size(), COUNT);
        List<Integer> back_list = back_result__list(graph, res);
       // print(list); print(back_list);
        Assert.assertTrue(reverseLists(list, back_list));
    }
    
    static List<Integer> result__list(HyperGraph graph, HGSearchResult<HGHandle> res)
    {
        List<Integer> list = new ArrayList<Integer> ();
        while(res.hasNext())
            list.add(((TestInt) graph.get(res.next())).getX());
       return list;
    }
    
    static boolean expectedType(Object o, String t)
    {
        return o.getClass().getName().indexOf(t) > -1;
    }
    static boolean isSortedList(List<Integer> list, boolean up)
    {
        if(list.isEmpty()) return true;
        int curr = list.get(0);
        for(int i = 1; i < list.size(); i++)
        {
            if(up && curr < list.get(i)
              || !up && curr > list.get(i))
               curr = list.get(i);
            else
               return false; 
        }
        return true;
    }
    
    static boolean reverseLists(List<Integer> list, List<Integer> other)
    {
       if(list.size() != other.size()) return false;
       int size = list.size();
       for(int i = 0; i < list.size(); i++)
           if(!list.get(i).equals(other.get(size-1-i)))
               return false;
       return true;        
    }
    
    static List<Integer> back_result__list(HyperGraph graph, HGSearchResult<HGHandle> res)
    {
        List<Integer> list = new ArrayList<Integer> ();
        //add the last result which is current right now
        Integer o = ((TestInt) graph.get(res.current())).getX();
        list.add(o);
        while(res.hasPrev())
            list.add(((TestInt) graph.get(res.prev())).getX());
       return list;
    }
       
  
    static void print(HyperGraph graph, HGSearchResult<HGHandle> res)
    {
       // System.out.println("HGSearchResult: " + res);
        List<Integer> list = result__list(graph, res);
        print(list);
    }
    
    static void print(List<Integer> list)
    {
       System.out.println("Reversed list");
       for (int i = 0; i < list.size(); i++)
            System.out.println(":" + i + ":" + list.get(i));
    }
    
    static void clean(HyperGraph graph)
    {
        List<HGHandle> list = hg.findAll(graph, hg.type(TestInt.class));
        for(HGHandle handle : list)
          graph.remove(handle);
        List<HGIndexer> indexers = graph.getIndexManager().getIndexersForType(
                graph.getTypeSystem().getTypeHandle(TestInt.class));
        for(HGIndexer indexer: indexers)
           graph.getIndexManager().deleteIndex(indexer);
    }
}
