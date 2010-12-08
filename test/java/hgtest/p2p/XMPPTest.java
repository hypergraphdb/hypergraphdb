package hgtest.p2p;

import static org.hypergraphdb.peer.Structs.getPart;

import hgtest.T;

import java.util.Map;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.xmpp.XMPPPeerInterface;

public class XMPPTest
{
	@SuppressWarnings("unchecked")
	public static void main(String [] argv)
	{
		String configFile = argv[0];
		Map<String, Object> config = (Map<String, Object>)getPart(
				new JSONReader().read(T.getResourceContents(configFile)));
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
