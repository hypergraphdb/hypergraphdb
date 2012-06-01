package hgtest.query;

import java.util.Set;

import hgtest.HGTestBase;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGSubgraph;
import org.testng.Assert;
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
					
		// Incident condition
		HGQuery<HGHandle> q = hg.make(HGHandle.class, getGraph()).compile(hg.incident(hg.var("target")));
		Set<HGHandle> S = q.var("target", h1).executeInSet();
		Assert.assertTrue(S.contains(l1)); 
		Assert.assertTrue(S.contains(l3));
		S = q.var("target", h2).executeInSet();
		Assert.assertTrue(S.contains(l2)); 
		Assert.assertTrue(S.contains(l3));
		
		// Type condition
		q = hg.make(HGHandle.class, graph).compile(hg.type(hg.var("type")));
		Assert.assertTrue(q.var("type", String.class).executeInList().size() > 0);
		Assert.assertTrue(q.var("type", graph.getTypeSystem().getTypeHandle(HGSubgraph.class)).executeInList().size() == 0);
		
		graph.remove(h1);
		graph.remove(h2);
		graph.remove(l1);
		graph.remove(l2);
		graph.remove(l3);
    }
}