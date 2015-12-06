package hgtest.p2p;

import java.io.File;
import java.net.InetAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import hgtest.beans.SimpleBean;
import hgtest.T;
import hgtest.beans.ComplexBean;
import hgtest.beans.PlainBean;
import mjson.Json;
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
import org.hypergraphdb.peer.bootstrap.AffirmIdentityBootstrap;
import org.hypergraphdb.peer.bootstrap.CACTBootstrap;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.RemoteQueryExecution;
import org.hypergraphdb.peer.cact.RunRemoteQuery;
import org.hypergraphdb.peer.cact.GetAtom;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCACT
{
	boolean both = true;
	private boolean which = false; // true for peer 1, falsefor peer 2
	private HyperGraph graph1, graph2;
	private HyperGraphPeer peer1, peer2;
	private File locationBase = new File(T.getTmpDirectory());
	private File locationGraph1 = new File(locationBase, "hgp2p1");
	private File locationGraph2 = new File(locationBase, "hgp2p2");

	private HyperGraphPeer startPeer(String dblocation, String username, String hostname)
	{
		hostname = "evalhalla.com";
		Json config = Json.object();
		config.set(PeerConfig.INTERFACE_TYPE, "org.hypergraphdb.peer.xmpp.XMPPPeerInterface");
		config.set(PeerConfig.LOCAL_DB, dblocation);
		Json interfaceConfig = Json.object();
		interfaceConfig.set("user", username);
		interfaceConfig.set("password", "hgpassword");
		interfaceConfig.set("serverUrl", hostname);
		interfaceConfig.set("room", "hgtest@conference.chat." + hostname);
		interfaceConfig.set("autoRegister", true);
		config.set(PeerConfig.INTERFACE_CONFIG, interfaceConfig);

		// bootstrap activities
		config.set(
				PeerConfig.BOOTSTRAP,
				Json.array(Json.object("class", AffirmIdentityBootstrap.class.getName(), "config", Json.object()),
						Json.object("class", CACTBootstrap.class.getName(), "config", Json.object())));

		HyperGraphPeer peer = new HyperGraphPeer(config);
		Future<Boolean> startupResult = peer.start();
		try
		{
			if (startupResult.get())
			{
				System.out.println("Peer " + username + " started successfully.");
			}
			else
			{
				System.out.println("Peer failed to start.");
				HGUtils.throwRuntimeException(peer.getStartupFailedException());
			}
		}
		catch (Exception e)
		{
			peer.stop();
			throw new RuntimeException(e);
		}
		return peer;
	}

	@BeforeClass
	public void setUp()
	{
		String localhost = "localhost";
		try
		{
			localhost = InetAddress.getLocalHost().getHostName();
		}
		catch (Throwable t)
		{
		}
		if (which || both)
		{
			HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
			graph1 = HGEnvironment.get(locationGraph1.getAbsolutePath());
			peer1 = startPeer(locationGraph1.getAbsolutePath(), "cact1", localhost);
		}
		if (!which || both)
		{
			HGUtils.dropHyperGraphInstance(locationGraph2.getAbsolutePath());
			graph2 = HGEnvironment.get(locationGraph2.getAbsolutePath());
			peer2 = startPeer(locationGraph2.getAbsolutePath(), "cact2", localhost);
		}
	}

	@AfterClass
	public void tearDown()
	{
		if (which || both)
		{
			try
			{
				peer1.stop();
			}
			catch (Throwable t)
			{
			}
			try
			{
				graph1.close();
			}
			catch (Throwable t)
			{
			}
			try
			{
				HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
			}
			catch (Throwable t)
			{
			}
		}
		if (!which || both)
		{
			try
			{
				peer2.stop();
			}
			catch (Throwable t)
			{
			}
			try
			{
				graph2.close();
			}
			catch (Throwable t)
			{
			}
			try
			{
				HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
			}
			catch (Throwable t)
			{
			}
		}
	}

	@Test
	public void testDefineAtom()
	{
		HGHandle fromPeer1 = graph1.add("From Peer1");
		peer1.getActivityManager().initiateActivity(new DefineAtom(peer1, fromPeer1, peer2.getIdentity()));
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
			GetAtom activity = new GetAtom(peer2, graph1.getPersistentHandle(fromPeer1), peer1.getIdentity());
			peer2.getActivityManager().initiateActivity(activity);
			activity.getFuture().get();
			Assert.assertEquals(activity.getState(), WorkflowState.Completed);
			Assert.assertEquals(activity.getOneAtom(), atom);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
			Assert.fail("Exception during GetAtom activity");
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
			GetAtom activity = new GetAtom(peer2, graph1.getPersistentHandle(fromPeer1), peer1.getIdentity());
			peer2.getActivityManager().initiateActivity(activity);
			activity.getFuture().get();
			Assert.assertEquals(activity.getState(), WorkflowState.Completed);
			Assert.assertEquals(activity.getOneAtom(), atom);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
			Assert.fail("Exception during GetAtom activity");
		}
	}

	@Test
	public void testRemoteQuery()
	{
		try
		{
			while (peer2.getConnectedPeers().isEmpty())
				Thread.sleep(500);

			HGQueryCondition expression = hg.dfs(graph1.getTypeSystem().getTop(), hg.type(HGSubsumes.class), null, false, true);
			RemoteQueryExecution<HGHandle> activity = new RemoteQueryExecution<HGHandle>(peer1, expression, peer2.getIdentity());
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
			t.printStackTrace();
			Assert.fail("Exception during RemoteQuery Activity activity");
		}
	}

	@Test
	public void testRemoteQueryBulk()
	{
		try
		{
			HGQueryCondition expression = hg.dfs(graph1.getTypeSystem().getTop(), 
												hg.type(HGSubsumes.class), null, false, true);
			RunRemoteQuery activity = new RunRemoteQuery(peer1, expression, false, -1, peer2.getIdentity());
			peer1.getActivityManager().initiateActivity(activity);
			activity.getFuture().get();
			@SuppressWarnings("unchecked")
			List<HGHandle> received = (List<HGHandle>) (List<?>) activity.getResult();
			List<HGHandle> expected = hg.findAll(graph2, expression); 
			Assert.assertEquals(received, expected);

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
				// System.out.println(((Pair<?, Object>)x).getSecond());
				result.add(((Pair<?, Object>) x).getSecond());
			}
			Assert.assertEquals(new HashSet<Object>(result), new HashSet<Object>(beans));
		}
		catch (Throwable t)
		{
			t.printStackTrace();
			Assert.fail("Exception during RemoteQuery Activity activity");
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
		HGHandle toBeReplaced = node.add(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6 }), listType, 0);

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
		Set<HGHandle> expected = new HashSet<HGHandle>();
		expected.addAll((List<HGHandle>)(List)node.findAll(hg.type(intType)));
		Set<HGHandle> actual = new HashSet<HGHandle>();
		actual.addAll(intHandles);
		Assert.assertEquals(expected, actual);
		Set<Integer> expectedValues = new HashSet<Integer>();
		expectedValues.addAll((List<Integer>)(List)node.getAll(hg.type(intType)));
		Set<Integer> actualValues = new HashSet<Integer>();
		actualValues.addAll(ints);
		Assert.assertEquals(expectedValues, actualValues);
		Assert.assertEquals(node.get(toBeReplaced), ints);
	}

	public static void main(String[] argv)
	{
		TestCACT test = new TestCACT();
		try
		{
			test.setUp();
			// if (test.which || test.both)
			// test.testRemoteQuery();
			// else
			// T.sleep(1000*60*60*5);
			while (test.peer2.getConnectedPeers().isEmpty() || test.peer1.getConnectedPeers().isEmpty())
				Thread.sleep(500);

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