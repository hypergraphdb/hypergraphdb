package hgtest.p2p;

import hgtest.T;
import mjson.Json;
import org.hypergraphdb.peer.HyperGraphPeer;

public class XMPPTest
{
	public static void main(String [] argv)
	{
		String configFile = argv[0];
		Json config = Json.read(T.getResourceContents(configFile));
		HyperGraphPeer peer = new HyperGraphPeer(config); 
//		XMPPPeerInterface xmpp = new XMPPPeerInterface();
//		xmpp.configure((Map)config.get("interfaceConfig"));
//		xmpp.start();
//		xmpp.setMessageHandler(new MessageHandler() 
//		{
//			public void handleMessage(Message msg)
//			{
//				System.out.println("Got message: " + msg);
//			}
//		}
//		);
		peer.start(null, null);
		while (true)
		try { Thread.sleep(2000); }
		catch (Throwable t) { }
	}
}
