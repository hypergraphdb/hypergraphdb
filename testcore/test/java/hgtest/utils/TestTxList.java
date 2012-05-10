package hgtest.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hypergraphdb.transaction.TxList;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;

public class TestTxList extends HGTestBase
{
    @Test
    public void testAddAbort()
    {
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        graph.getTransactionManager().beginTransaction();
        L.add("blabla");
        graph.getTransactionManager().abort();
        Assert.assertEquals(L.size(), 0);
    }
    
    @Test
    public void testOneAdd()
    {
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        graph.getTransactionManager().beginTransaction();
        L.add("blabla");
        graph.getTransactionManager().commit();
        Assert.assertEquals(L.size(), 1);
        Assert.assertEquals(L.get(0), "blabla");
    }
    
    @Test
    public void testManyAdd()
    {
        List<String> base = Arrays.asList("first", "second", "third", "forth", "fifth", "sixth", "sevent", "eight");
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        graph.getTransactionManager().beginTransaction();
        for (String s : base)
            L.add(s);
        graph.getTransactionManager().commit();
        Assert.assertEquals(L.size(), base.size());
        Assert.assertEquals(L.toArray(), base.toArray());        
    }
    
    @Test
    public void testRemoveAbort()
    {
        List<String> base = Arrays.asList("first", "second", "third", "forth", "fifth", "sixth", "sevent", "eight");
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        graph.getTransactionManager().beginTransaction();
        for (String s : base)
            L.add(s);
        graph.getTransactionManager().commit();
        
        graph.getTransactionManager().beginTransaction();
        L.remove(2);
        L.add("afgasdffgasdF");
        L.remove("sixth");
        graph.getTransactionManager().abort();
        
        Assert.assertEquals(L.size(), base.size());
        Assert.assertEquals(L.toArray(), base.toArray());                
    }

    @Test
    public void testRemove()
    {
        ArrayList<String> base = new ArrayList<String>();
        base.addAll(Arrays.asList("first", "second", "third", "forth", "fifth", "sixth", "sevent", "eight"));
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        graph.getTransactionManager().beginTransaction();
        for (String s : base)
            L.add(s);
        graph.getTransactionManager().commit();
        
        graph.getTransactionManager().beginTransaction();
        L.remove(2);
        L.remove("sixth");
        base.remove(2);
        base.remove("sixth");
        graph.getTransactionManager().commit();        
        
        Assert.assertEquals(L.size(), base.size());
        Assert.assertEquals(L.toArray(), base.toArray());                
    }

    @Test
    public void testPositionalAdd()
    {
        List<String> base = Arrays.asList("first", "second", "third", "forth", "fifth", "sixth", "sevent", "eight");
        TxList<String> L = new TxList<String>(graph.getTransactionManager());
        
        graph.getTransactionManager().beginTransaction();
        L.add(0, "first");
        L.add(1, "second");
        L.addAll(2, Arrays.asList("third", "forth", "fifth", "sixth"));
        L.add(L.size(), "eight");
        L.add(L.size() - 1, "sevent");
        graph.getTransactionManager().commit();
        
        Assert.assertEquals(L.size(), base.size());
        Assert.assertEquals(L.toArray(), base.toArray());        
    }
}