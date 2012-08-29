package hgtest.p2p;


import java.util.concurrent.Future;
import mjson.Json;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.testng.annotations.Test;
import hgtest.HGTestBase;

public class PeerNoDB extends HGTestBase
{
	@Test
	public void testPeerWithoutLocalDB()
	{
		Json config = Json.object();
		config.set("interfaceType", "org.hypergraphdb.peer.xmpp.XMPPPeerInterface");
		Json interfaceConfig = Json.object();
		interfaceConfig.set("user", "hgtest");
		interfaceConfig.set("password", "hgpassword");
		interfaceConfig.set("serverUrl", "localhost");
		interfaceConfig.set("room", "play@conference.ols00068");
		interfaceConfig.set("autoRegister", true);
		config.set("interfaceConfig", interfaceConfig);
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