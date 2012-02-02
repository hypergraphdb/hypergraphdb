package hgtest.p2p;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import hgtest.HGTestBase;
import hgtest.SimpleBean;
import hgtest.T;
import hgtest.beans.ComplexBean;
import hgtest.beans.PlainBean;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HyperNode;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.PeerHyperNode;
import org.hypergraphdb.peer.Structs;
import org.hypergraphdb.peer.bootstrap.AffirmIdentityBootstrap;
import org.hypergraphdb.peer.bootstrap.CACTBootstrap;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.RemoteQueryExecution;
import org.hypergraphdb.peer.cact.RunRemoteQuery;
import org.hypergraphdb.peer.workflow.AffirmIdentity;
import org.hypergraphdb.peer.workflow.StateListener;
import org.hypergraphdb.peer.cact.GetAtom;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCACT
{
    private HyperGraph graph1, graph2;
    private HyperGraphPeer peer1, peer2;
    private File locationBase = new File(T.getTmpDirectory());
    private File locationGraph1 = new File(locationBase, "hgp2p1");
    private File locationGraph2 = new File(locationBase, "hgp2p2");

    private HyperGraphPeer startPeer(String dblocation, String username,
                                     String hostname)
    {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(PeerConfig.INTERFACE_TYPE,
                   "org.hypergraphdb.peer.xmpp.XMPPPeerInterface");
        config.put(PeerConfig.LOCAL_DB, dblocation);
        Map<String, Object> interfaceConfig = new HashMap<String, Object>();
        interfaceConfig.put("user", username);
        interfaceConfig.put("password", "hgpassword");
        interfaceConfig.put("serverUrl", hostname);
        interfaceConfig.put("room", "hgtest@conference." + hostname);
        interfaceConfig.put("autoRegister", true);
        config.put(PeerConfig.INTERFACE_CONFIG, interfaceConfig);

        // bootstrap activities
        config.put(PeerConfig.BOOTSTRAP,
                   Structs.list(Structs.struct("class",
                                               AffirmIdentityBootstrap.class.getName(),
                                               "config",
                                               Structs.struct()),
                                Structs.struct("class",
                                               CACTBootstrap.class.getName(),
                                               "config",
                                               Structs.struct())));

        HyperGraphPeer peer = new HyperGraphPeer(config);
        Future<Boolean> startupResult = peer.start();
        try
        {
            if (startupResult.get())
            {
                System.out.println("Peer " + username
                        + " started successfully.");
            }
            else
            {
                System.out.println("Peer failed to start.");
                HGUtils.throwRuntimeException(peer.getStartupFailedException());
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return peer;
    }

    @BeforeClass
    public void setUp()
    {
        HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
        HGUtils.dropHyperGraphInstance(locationGraph2.getAbsolutePath());
        graph1 = HGEnvironment.get(locationGraph1.getAbsolutePath());
        graph2 = HGEnvironment.get(locationGraph2.getAbsolutePath());
        peer1 = startPeer(locationGraph1.getAbsolutePath(), "cact1", "samizdat");
        peer2 = startPeer(locationGraph2.getAbsolutePath(), "cact2", "samizdat");
    }

    @AfterClass
    public void tearDown()
    {
        peer1.stop();
        peer2.stop();
        graph1.close();
        graph2.close();
        HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
        HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
    }

    @Test
    public void testDefineAtom()
    {
        HGHandle fromPeer1 = graph1.add("From Peer1");
        peer1.getActivityManager().initiateActivity(new DefineAtom(peer1,
                fromPeer1, peer2.getIdentity()));
        T.sleep(2000);
        String received = graph2.get(graph1.getPersistentHandle(fromPeer1));
        if (received != null)
            System.out.println("Peer 2 received " + received);
        else
            Assert.fail("peer 2 didn't received message");
    }

    @Test
    public void testGetAtom()
    {
        Object atom = "Get Atom In Peer1";
        HGHandle fromPeer1 = hg.assertAtom(graph1, atom);
        try
        {
            while (peer2.getConnectedPeers().isEmpty())
                Thread.sleep(500);
            GetAtom activity = new GetAtom(peer2,
                    graph1.getPersistentHandle(fromPeer1), peer1.getIdentity());
            peer2.getActivityManager().initiateActivity(activity);
            activity.getFuture().get();
            Assert.assertEquals(activity.getState(), WorkflowState.Completed);
            Assert.assertEquals(activity.getOneAtom(), atom);
        }
        catch (Throwable t)
        {
            Assert.fail("Exception during GetAtom activity", t);
        }
    }

    @Test
    public void testRemoveAtom()
    {
        Object atom = "Get Atom In Peer1";
        HGHandle fromPeer1 = hg.assertAtom(graph1, atom);
        try
        {
            while (peer2.getConnectedPeers().isEmpty())
                Thread.sleep(500);
            GetAtom activity = new GetAtom(peer2,
                    graph1.getPersistentHandle(fromPeer1), peer1.getIdentity());
            peer2.getActivityManager().initiateActivity(activity);
            activity.getFuture().get();
            Assert.assertEquals(activity.getState(), WorkflowState.Completed);
            Assert.assertEquals(activity.getOneAtom(), atom);
        }
        catch (Throwable t)
        {
            Assert.fail("Exception during GetAtom activity", t);
        }
    }

    @Test
    public void testRemoteQuery()
    {
        try
        {
            while (peer2.getConnectedPeers().isEmpty())
                Thread.sleep(500);
            
            HGQueryCondition expression = hg.dfs(graph1.getTypeSystem().getTop(), 
                                               hg.type(HGSubsumes.class), null, false, true); 
            RemoteQueryExecution<HGHandle> activity = 
                new RemoteQueryExecution<HGHandle>(peer1, expression, peer2.getIdentity());
            peer1.getActivityManager().initiateActivity(activity);
            activity.getState().getFuture(RemoteQueryExecution.ResultSetOpen).get();
            List<HGHandle> received = new ArrayList<HGHandle>(); 
            HGSearchResult<HGHandle> rs = activity.getSearchResult();
            while (rs.hasNext())
                received.add(rs.next());
            rs.close();
            Assert.assertEquals(hg.findAll(graph2, expression), received);                
        }
        catch (Throwable t)
        {
            Assert.fail("Exception during RemoteQuery Activity activity", t);
        }        
    }

    @Test
    public void testRemoteQueryBulk()
    {
        try
        {
            while (peer2.getConnectedPeers().isEmpty())
                Thread.sleep(500);
            
            HGQueryCondition expression = hg.dfs(graph1.getTypeSystem().getTop(), 
                                               hg.type(HGSubsumes.class), null, false, true); 
            RunRemoteQuery activity = 
                new RunRemoteQuery(peer1, expression, false, -1, peer2.getIdentity());
            peer1.getActivityManager().initiateActivity(activity);
            activity.getFuture().get();
            @SuppressWarnings("unchecked")
            List<HGHandle> received = (List<HGHandle>)(List<?>)activity.getResult(); 
            Assert.assertEquals(hg.findAll(graph2, expression), received);
            
            List<Object> beans = new ArrayList<Object>();
            for (int i = 0; i < 158; i++)
                beans.add(new PlainBean(i));
            for (Object x : beans)
                graph2.add(x);
            
            activity = new RunRemoteQuery(peer1, hg.type(PlainBean.class), true, -1, peer2.getIdentity());
            peer1.getActivityManager().initiateActivity(activity);
            activity.getFuture().get();
            
            List<Object> result = new ArrayList<Object>();
            for (Object x : activity.getResult())
            {
//                System.out.println(((Pair<?, Object>)x).getSecond());
                result.add(((Pair<?, Object>)x).getSecond());
            }
            Assert.assertEqualsNoOrder(result.toArray(), beans.toArray());
        }
        catch (Throwable t)
        {
            Assert.fail("Exception during RemoteQuery Activity activity", t);
        }        
    }
    
    @Test
    public void testHyperNode() throws Throwable
    {
        while (peer2.getConnectedPeers().isEmpty())
            Thread.sleep(500);
        HyperNode node = new PeerHyperNode(peer1, peer2.getIdentity());
        
        HGHandle stringType = graph1.getTypeSystem().getTypeHandle(String.class);
        HGHandle beanType = graph1.getTypeSystem().getTypeHandle(ComplexBean.class);
        HGHandle intType = graph1.getTypeSystem().getTypeHandle(Integer.class);
        HGHandle listType = graph1.getTypeSystem().getTypeHandle(ArrayList.class);
        
        HGHandle x1 = node.add("Hello World", stringType, 0);
        HGHandle x2 = graph1.getHandleFactory().makeHandle();
        ComplexBean complexBean = new ComplexBean();
        complexBean.setStableField("HYPERNODE");
        complexBean.setStableNested(new SimpleBean());
        node.define(x2, beanType, complexBean, 0);
        
        HGHandle toBeRemoved = node.add(10, intType, 0);
        HGHandle toBeReplaced = node.add(Arrays.asList(new Integer[]{1,2,3,4,5,6}), listType, 0);
        
        ArrayList<Integer> ints = new ArrayList<Integer>();
        for (int i = 0; i < 37; i++)
        {
            ints.add(i);
            node.add(i, intType, 0);
        }
        
        node.replace(toBeReplaced, ints, listType);
        
        Assert.assertTrue(node.remove(toBeRemoved));        
        Assert.assertEquals(node.count(hg.eq(10)), 1);
        Assert.assertEquals(node.get(x1), "Hello World");
        Assert.assertEquals(node.get(x2), complexBean);
        
        HGSearchResult<HGHandle> rs = node.find(hg.type(intType));
        Set<Integer> intSet = new HashSet<Integer>();
        intSet.addAll(ints);  
        ArrayList<HGHandle> intHandles = new ArrayList<HGHandle>();
        while (rs.hasNext())
        {
            Assert.assertTrue(intSet.contains(node.get(rs.next())));
            intSet.remove(node.get(rs.current()));
            intHandles.add(rs.current());
            if (rs.hasPrev())
            {
                rs.prev();
                rs.next();
            }
        }
        rs.close();
        Assert.assertTrue(intSet.isEmpty());
        Assert.assertEqualsNoOrder(node.findAll(hg.type(intType)).toArray(new HGHandle[0]), 
                                                intHandles.toArray(new HGHandle[0]));
        Integer[] II = node.getAll(hg.type(intType)).toArray(new Integer[0]);
        Assert.assertEqualsNoOrder(II, ints.toArray(new Integer[0]));
        Assert.assertEquals(node.get(toBeReplaced), ints);
    }
    
    public static void main(String[] argv)
    {
        TestCACT test = new TestCACT();
        try
        {
            test.setUp();
            test.testHyperNode();
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