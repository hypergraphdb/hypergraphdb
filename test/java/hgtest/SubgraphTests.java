package hgtest;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.HGSubgraph;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubgraphTests extends HGTestBase
{
    @Test
    public void testSubgraphOperations()
    {
        HGSubgraph subgraph = new HGSubgraph();
        graph.add(subgraph);
        
        HGHandle stringType = graph.getTypeSystem().getTypeHandle(String.class);
        HGHandle globalAtom = graph.add("global");
        subgraph.add(globalAtom);
        Assert.assertTrue(subgraph.isMember(globalAtom));
        HGHandle globalOnly = graph.add("globalOnly");
        
        HGHandle localAtom = subgraph.add("localAtom", stringType, 0);
        Assert.assertTrue(subgraph.isMember(localAtom));
        HGHandle localDefine = graph.getHandleFactory().makeHandle();
        subgraph.define(localDefine, stringType, "localDefinedAtom", 0);
        Assert.assertTrue(subgraph.isMember(localDefine));
        
        HGHandle toBeRemoved = subgraph.add("toBeRemoved", stringType, 0);
        Assert.assertTrue(subgraph.isMember(toBeRemoved));
        subgraph.remove(toBeRemoved);
        HGHandle toBeReplaced = subgraph.add("toBeReplaced", stringType, 0);
        Assert.assertTrue(subgraph.get(toBeReplaced).equals("toBeReplaced"));
                
        this.reopenDb();
        
        subgraph = graph.get(subgraph.getAtomHandle());        
        Assert.assertNull(subgraph.get(globalOnly));
        Assert.assertNull(subgraph.findOne(hg.eq("toBeRemoved")));
        Assert.assertNotNull(graph.findOne(hg.eq("toBeRemoved")));
        subgraph.replace(toBeReplaced, "alreadyReplaced", stringType);
        Assert.assertEquals(graph.getOne(hg.eq("toBeReplaced")), null);

        Assert.assertEqualsNoOrder(subgraph.getAll(hg.type(String.class)).toArray(), 
                            new Object[] {"global", "localAtom", "localDefinedAtom", "alreadyReplaced" });
    }
    
    public static void main(String[] argv)
    {
        SubgraphTests test = new SubgraphTests();
        try
        {
            test.setUp();
            test.testSubgraphOperations();
            System.out.println("test passed successfully");
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
