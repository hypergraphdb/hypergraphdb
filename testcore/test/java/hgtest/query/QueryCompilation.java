package hgtest.query;

import hgtest.HGTestBase;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.testng.annotations.Test;

public class QueryCompilation extends HGTestBase
{
    @Test
    public void testVariableReplacement()
    {
	HyperGraph graph = getGraph();
	HGHandle h1 = graph.add("target1");
	HGHandle h2 = graph.add("target2");
	HGHandle l1 = graph.add(new HGPlainLink(h1));
	HGHandle l2 = graph.add(new HGPlainLink(h2));
	HGHandle l3 = graph.add(new HGPlainLink(h1, h2));

	HGHandle h = graph.getHandleFactory().makeHandle();				
	HGQuery<?> q = HGQuery.make(getGraph(), hg.and(hg.type(String.class), hg.incident(hg.var(HGHandle.class, "target"))));
	q.var("target", h).execute();

	graph.remove(h1);
	graph.remove(h2);
	graph.remove(l1);
	graph.remove(l2);
	graph.remove(l3);
    }
}