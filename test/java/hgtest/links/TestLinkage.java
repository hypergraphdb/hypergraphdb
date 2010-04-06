package hgtest.links;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HGQuery.hg;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import static hgtest.verify.HGAssert.*;

public class TestLinkage extends HGTestBase
{
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
}