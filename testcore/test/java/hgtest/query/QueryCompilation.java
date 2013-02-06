package hgtest.query;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import hgtest.HGTestBase;
import hgtest.beans.Car;
import hgtest.beans.SimpleBean;
import hgtest.beans.Transport;
import hgtest.utils.DataSets;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGSubgraph;
import org.hypergraphdb.util.Ref;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryCompilation extends HGTestBase
{
    @Test
    public void testNonExistingTypeQuery()
    {
        Assert.assertTrue(hg.findAll(getGraph(), hg.typePlus(Transport.class))
                .isEmpty());
        Assert.assertTrue(hg.findAll(getGraph(), hg.type(Car.class)).isEmpty());
    }

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
        HGQuery<HGHandle> q = hg.make(HGHandle.class, getGraph()).compile(
                hg.incident(hg.var("target")));
        Set<HGHandle> S = q.var("target", h1).findInSet();
        Assert.assertTrue(S.contains(l1));
        Assert.assertTrue(S.contains(l3));
        S = q.var("target", h2).findInSet();
        Assert.assertTrue(S.contains(l2));
        Assert.assertTrue(S.contains(l3));

        // Type condition
        q = hg.make(HGHandle.class, graph).compile(hg.type(hg.var("type")));
        Assert.assertTrue(q.var("type", String.class).findAll().size() > 0);
        Assert.assertTrue(q.var("type",
                graph.getTypeSystem().getTypeHandle(HGSubgraph.class))
                .findAll().size() == 0);

        // Equals
        HGHandle nameHandle = graph.add("krokus");
        HGQuery<HGHandle> findName = HGQuery.make(HGHandle.class, graph)
                .compile(hg.and(hg.type(String.class), hg.eq(hg.var("name"))));
        Assert.assertEquals(nameHandle, findName.var("name", "krokus")
                .findOne());

        graph.remove(h1);
        graph.remove(h2);
        graph.remove(l1);
        graph.remove(l2);
        graph.remove(l3);
    }

    @Test
    public void testRepeatedConcurrentQueries()
    {
        DataSets.populate(graph, 100 * 100);
        ExecutorService eservice = Executors.newFixedThreadPool(20);

        final HGHandle nodeHandle = hg.findOne(graph, hg.and(hg
                .type(SimpleBean.class), hg.eq("intProp", 50)));
        Assert.assertNotNull(nodeHandle);

        final HGQuery<HGHandle> q1 = hg.make(HGHandle.class, graph).compile(
                hg.and(hg.typePlus(HGPlainLink.class), hg.incidentNotAt(hg.var(
                        "firstTarget", HGHandle.class), 1)));

        final HGQuery<SimpleBean> q2 = HGQuery
                .make(SimpleBean.class, graph)
                .compile(hg.and(hg.type(SimpleBean.class), hg.eq("intProp", 3)));

        for (int i = 0; i < 100; i++)
        {
            final int ii = i;
            eservice.submit(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        q1.var("firstTarget", nodeHandle);
                        Assert.assertNotNull(q1.findOne());
                        System.out.println("Done with " + ii);
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace(System.err);
                    }
                }
            });
        }
        try
        {
            eservice.shutdown();
            eservice.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    @Test
    public void testApplyTargetAt()
    {
        HyperGraph graph = getGraph();
        HGQuery<?> incomingIncidentNodesQuery = hg.make(HGHandle.class, graph)
                .compile(
                        hg.apply(hg.targetAt(graph, 0), hg.and(hg.eq(hg.var("edgeLabel")),
                                hg.incidentAt(hg.var("node", HGHandle.class), 1))));
        incomingIncidentNodesQuery.var("edgeLabel", "asdfasd").var("node", graph.getHandleFactory().makeHandle());        
        incomingIncidentNodesQuery.execute().close();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testOLC()
    {
        HyperGraph graph = getGraph();
        HGQuery<?>  q = hg.make(HGHandle.class, graph)
                .compile(
                    hg.and(
                        hg.eq("L1"),
                        hg.orderedLink(
                            hg.var("aNode", HGHandle.class),
                            hg.var("bNode", HGHandle.class)
                        )
                    )
                ); 
        System.out.println(q.findInSet());
    }
}