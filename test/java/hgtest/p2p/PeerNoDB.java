package hgtest.p2p;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.testng.annotations.Test;
import hgtest.HGTestBase;

public class PeerNoDB extends HGTestBase
{
	@Test
	public void testPeerWithoutLocalDB()
	{
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("interfaceType", "org.hypergraphdb.peer.xmpp.XMPPPeerInterface");
		Map<String, Object> interfaceConfig = new HashMap<String, Object>();
		interfaceConfig.put("user", "hgtest");
		interfaceConfig.put("password", "hgpassword");
		interfaceConfig.put("serverUrl", "localhost");
		interfaceConfig.put("room", "play@conference.ols00068");
		interfaceConfig.put("autoRegister", true);
		config.put("interfaceConfig", interfaceConfig);
		HyperGraphPeer peer = new HyperGraphPeer(config); 
	    Future<Boolean> startupResult = peer.start();		
	    try
		{
			if (startupResult.get())
			{
			    System.out.println("Peer started successfully.");
			}
			else
			{
			    System.out.println("Peer failed to start.");
			    peer.getStartupFailedException().printStackTrace(System.err);
			}
		} 
	    catch (Exception e)
		{
			e.printStackTrace(System.err);
		} 
	}
}