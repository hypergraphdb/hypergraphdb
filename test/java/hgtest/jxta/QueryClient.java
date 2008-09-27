package hgtest.jxta;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.jxta.platform.NetworkManager;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.RemotePeer;
import org.hypergraphdb.peer.jxta.DefaultPeerFilterEvaluator;

public class QueryClient
{
	public static void main(String[] args) throws NumberFormatException, IOException{
		System.out.println("Starting a HGDB client ...");

		HyperGraphPeer peer = new HyperGraphPeer(new File("./config/client1Config"));
		
		peer.start("user", "pwd");

		try
		{
			Thread.sleep(3000);
		} catch (InterruptedException e){}
	
		HGPersistentHandle typeHandle = UUIDPersistentHandle.makeHandle("e917bda6-0932-4a66-9aeb-3fc84f04ce57");
		peer.registerType(typeHandle, User.class);
		System.out.println("Types registered...");

		peer.updateNetworkProperties();
		
		
		RemotePeer remotePeer = peer.getRemotePeer("Server1");
		if (remotePeer != null)
		{
			ArrayList<?> result = remotePeer.query(hg.type(User.class), false);
			
			//getting users from Server1
			for(Object elem:result)
			{
				HGHandle handle = (HGHandle)elem;
				System.out.println("received: " + elem + " -> " + remotePeer.get(handle));
			}
		}
		
		HGHandle addedHandle = remotePeer.add(new User(11, "user 11"));
		System.out.println("added: " + addedHandle);
		System.out.println("the value = " + remotePeer.get(addedHandle));
		
		remotePeer.replace((HGPersistentHandle)addedHandle, new User(11, "new user 11"));
		System.out.println("updated: " + addedHandle);
		System.out.println("the value = " + remotePeer.get(addedHandle));
		
		
		remotePeer.remove((HGPersistentHandle)addedHandle);
		System.out.println("removed: " + addedHandle);
		System.out.println("the value = " + remotePeer.get(addedHandle));
		
		HGPersistentHandle specificHandle = UUIDPersistentHandle.makeHandle();
		remotePeer.define(specificHandle, new User(12, "user 12"));

		System.out.println("defined: " + specificHandle + " -> " + remotePeer.get(specificHandle));
		
		HGHandle copiedHandle = peer.getHGDB().add(new User(14, "user 14"));
		remotePeer.copyTo(copiedHandle);
		
		System.out.println("copied: " + copiedHandle + " -> " + remotePeer.get(peer.getHGDB().getPersistentHandle(copiedHandle)));
		
		peer.getHGDB().replace(copiedHandle, new User(14, "new user 14"));
		remotePeer.copyTo(copiedHandle);
		
		System.out.println("copied: " + copiedHandle + " -> " + remotePeer.get(peer.getHGDB().getPersistentHandle(copiedHandle)));
		

		HGPersistentHandle remoteHandle = (HGPersistentHandle)remotePeer.add(new User(15, "user 15"));
		remotePeer.copyFrom(remoteHandle);
		
		System.out.println("copied: " + remoteHandle + " -> " + peer.getHGDB().get(remoteHandle));
		
		remotePeer.replace(remoteHandle, new User(15, "new user 15"));
		remotePeer.copyFrom(remoteHandle);
		
		System.out.println("copied: " + remoteHandle + " -> " + peer.getHGDB().get(remoteHandle));
	
		
		remotePeer.startBatch();
		
		remotePeer.add(new User(16, "user 16"));
		remotePeer.add(new User(17, "user 17"));
		
		List<HGHandle> result = (List<HGHandle>)remotePeer.flushBatch();
		System.out.println("batch results: " + result);
		
		remotePeer.add(new User(18, "user 18"));
		HGPersistentHandle handle = UUIDPersistentHandle.makeHandle();
		remotePeer.define(handle, new User(19, "user 19"));
		
		List<HGHandle> result1 = (List<HGHandle>)remotePeer.flushBatch();
		System.out.println("batch results: " + result1);

		System.out.println("defined: " + handle + " -> " + remotePeer.get(handle));
		
//		result = peer.query(new DefaultPeerFilterEvaluator("Server1"), hg.type(User.class), true);
//		System.out.println("the client received: " + result);
	}
}
