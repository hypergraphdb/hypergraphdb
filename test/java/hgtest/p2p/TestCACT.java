package hgtest.p2p;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import hgtest.T;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.Structs;
import org.hypergraphdb.peer.bootstrap.AffirmIdentityBootstrap;
import org.hypergraphdb.peer.bootstrap.CACTBootstrap;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.workflow.AffirmIdentity;
import org.hypergraphdb.util.HGUtils;
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

	private HyperGraphPeer startPeer(String dblocation, String username, String hostname)
	{
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(PeerConfig.INTERFACE_TYPE, "org.hypergraphdb.peer.xmpp.XMPPPeerInterface");
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
				   Structs.list(Structs.struct("class", AffirmIdentityBootstrap.class.getName(), 
						   					   "config", Structs.struct()),
						   	    Structs.struct("class", CACTBootstrap.class.getName(), 
						   	    				"config", Structs.struct())
						   	    )
				);
		
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
			throw new RuntimeException(e);
		} 
		return peer;
	}
	
    @BeforeClass
    public void setUp()
    {
        HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
        HGUtils.dropHyperGraphInstance(locationGraph1.getAbsolutePath());
        graph1 = HGEnvironment.get(locationGraph1.getAbsolutePath());        
        graph2 = HGEnvironment.get(locationGraph2.getAbsolutePath());
        peer1 = startPeer(locationGraph1.getAbsolutePath(), "cact1", "ols00068");
        peer2 = startPeer(locationGraph2.getAbsolutePath(), "cact2", "ols00068");
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
    	peer1.getActivityManager().initiateActivity(
    		new DefineAtom(peer1, fromPeer1, peer2.getIdentity()));
    	T.sleep(2000);
    	String received = graph2.get(graph1.getPersistentHandle(fromPeer1));
    	if (received != null)
    		System.out.println("Peer 2 received " + received);
    	else
    		Assert.fail("peer 2 didn't received message");
    }
}