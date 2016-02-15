package hgtest.links;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HGQuery.hg;
import org.junit.Assert;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.beans.SimpleBean;
import static hgtest.verify.HGAssert.*;

public class TestLinkage extends HGTestBase
{
    public static class MyClass {
        
        private String id;
      
        public MyClass() {
        }
      
        public MyClass(String id) {
          this.id = id;
        }
   }

    @Test
    public void testDefineValueLink()
    {
        HGHandle handle = graph.getHandleFactory().makeHandle();
        MyClass linkValue = new MyClass(handle.getPersistent().toString());

        HGLink link = new HGValueLink(linkValue, graph.add("some object"));
        graph.define(handle, link);
        Assert.assertEquals(graph.getHandle(link), handle);
    }
    
    @Test
    public void testValueLink()
    {
        HGHandle x1 = graph.add("atom1");
        HGHandle x2 = graph.add("atom2");
        HGHandle x3 = graph.add("atom3");
        HGHandle l = graph.add(new HGValueLink("testvaluelink", x1, x2, x3));
        with(graph)
            .isTrue(l, hg.orderedLink(x1, x2, x3))
            .isTrue(l, hg.arity(3))
            .isTrue(x1, hg.arity(0))
            .isTrue(x2, hg.arity(0))
            .isTrue(x3, hg.arity(0))
            .valueOf(l, "testvaluelink")
            .subgraph(x1, x2, x3, l);
    }
    
    @Test 
    public void testSimpleConnection()
    {
        HGHandle x1 = hg.assertAtom(graph, "atom1");
        HGHandle x2 = hg.assertAtom(graph, "atom2");
        HGHandle x3 = hg.assertAtom(graph, "atom3");
        HGHandle l1 = hg.assertAtom(graph, new HGPlainLink(x1, x2));
        HGHandle l2 = hg.assertAtom(graph, new HGPlainLink(x2, x3));        
        Assert.assertTrue(graph.findOne(hg.and(hg.bfs(x1), hg.is(x3))).equals(x3));
    }
}