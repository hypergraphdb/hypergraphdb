package hgtest.query;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.HGUtils;

import static org.hypergraphdb.HGQuery.*;

import static org.junit.Assert.*;
import org.junit.Test;
import hgtest.HGTestBase;

public class PatternTests extends HGTestBase
{
    
    @Test
    public void testCommonAdjacencyPattern()
    {
        HGHandle a = graph.add("A");
        HGHandle b = graph.add("B");
        HGHandle c1 = graph.add("C1");
        HGHandle c2 = graph.add("C2");
        HGHandle c3 = graph.add("C3");
        HGHandle c4 = graph.add("C4");
        HGHandle c5 = graph.add("C5");
        
        graph.add(new HGPlainLink(c1, a));
        graph.add(new HGPlainLink(c1, b));
        graph.add(new HGPlainLink(c1, c2));
        graph.add(new HGPlainLink(c2, a));
        graph.add(new HGPlainLink(c2, c3));
        graph.add(new HGPlainLink(c4, a));
        graph.add(new HGPlainLink(c4, b));
        graph.add(new HGPlainLink(c5, b));
        graph.add(new HGPlainLink(c5, b));
        graph.add(new HGPlainLink(c5, c2));                
        
        HGQueryCondition cond = hg.and(
                                       hg.type(String.class), 
                                       hg.and(
                                           hg.apply(
                                               hg.targetAt(graph, 0),                                          // follow target[0] of the OrderedLink
                                               hg.orderedLink(hg.anyHandle(), a)              // links pointing to A
                                           ),
                                           hg.apply(
                                               hg.targetAt(graph, 0),                                          // follow target[0] of the OrderedLink
                                               hg.orderedLink(hg.anyHandle(), b)             // links pointing to B
                                           )
                                       )
                              );         
        List<HGHandle> L = hg.findAll(graph, cond);
        
        assertTrue(L.contains(c1));
        assertFalse(L.contains(c2));
        assertFalse(L.contains(c3));
        assertTrue(L.contains(c4));
        assertFalse(L.contains(c5));
    }
    
    public static void main(String[] argv)
    {
        PatternTests test = new PatternTests();
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.testCommonAdjacencyPattern();
            System.out.println("Tests completed successfully.");
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
            test.tearDown();
        }        
    }
}