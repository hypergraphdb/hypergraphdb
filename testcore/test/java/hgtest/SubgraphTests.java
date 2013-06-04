package hgtest;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.HGSubgraph;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubgraphTests extends HGTestBase
{
    @Test
    public void testSubgraphTypeIndex()
    {
        
    }
    
    @Test
    public void testSubgraphOperations()
    {
        HGSubgraph subgraph = new HGSubgraph();
        graph.add(subgraph);

        HGHandle stringType = graph.getTypeSystem().getTypeHandle(String.class);
        HGHandle linkType = graph.getTypeSystem().getTypeHandle(HGPlainLink.class);
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
        
        // A b/w atoms in the subgraph, attached itself to the subgraph
        HGHandle localLink1 = subgraph.add(new HGPlainLink(localAtom, globalAtom), linkType, 0);
        // A b/w atoms not all in the subgraph, but still attached itself to the subgraph
        HGHandle localLink2 = subgraph.add(new HGPlainLink(globalOnly, localAtom), linkType, 0);
        // A b/w atoms in the subgraph, but not attached itself to the subgraph
        HGHandle globalLink = graph.add(new HGPlainLink(localDefine, globalAtom), linkType, 0);
        HGHandle globalLink2 = graph.add(new HGPlainLink(globalOnly, localDefine, globalAtom), linkType, 0);
        Assert.assertFalse(subgraph.getIncidenceSet(localDefine).contains(globalLink2));
        
        this.reopenDb();
        
        stringType = graph.getTypeSystem().getTypeHandle(String.class);
        subgraph = graph.get(subgraph.getAtomHandle().getPersistent());        
        Assert.assertNull(subgraph.get(globalOnly));
        Assert.assertNull(subgraph.findOne(hg.eq("toBeRemoved")));
        Assert.assertNotNull(graph.findOne(hg.eq("toBeRemoved")));
        subgraph.replace(toBeReplaced, "alreadyReplaced", stringType);
        Assert.assertEquals(graph.getOne(hg.eq("toBeReplaced")), null);

        Assert.assertEqualsNoOrder(subgraph.getAll(hg.type(String.class)).toArray(), 
                            new Object[] {"global", "localAtom", "localDefinedAtom", "alreadyReplaced" });
        
        // Checks links and incidence sets
        Assert.assertTrue(subgraph.isMember(localLink1));
        Assert.assertTrue(subgraph.isMember(localLink2));
        Assert.assertFalse(subgraph.isMember(globalLink));
        
        Assert.assertEquals(subgraph.getIncidenceSet(localAtom).size(), 2);
        Assert.assertEquals(subgraph.getIncidenceSet(globalAtom).size(), 1);
        Assert.assertEquals(subgraph.getIncidenceSet(localDefine).size(), 0);
        Assert.assertEquals(subgraph.getIncidenceSet(globalOnly).size(), 1);
        
        Assert.assertEqualsNoOrder(subgraph.getIncidenceSet(localAtom).toArray(), 
                                   new Object[] { localLink1, localLink2 });
        
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
