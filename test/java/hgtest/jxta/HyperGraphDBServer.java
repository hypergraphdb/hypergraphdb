package hgtest.jxta;

import java.io.File;
import java.util.concurrent.ExecutionException;

import net.jxta.platform.NetworkManager;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.RemotePeer;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.ComparisonOperator;

public class HyperGraphDBServer {
	public static void main(String[] args){
		if (args.length != 3)
		{
			System.out.println("arguments: configFile startId userCount");
			System.exit(0);
		}
		
		String configFile = args[0];
		int startId = Integer.parseInt(args[1]);
		int count = Integer.parseInt(args[2]);
		
		
		System.out.println("Starting HGDB peer " + configFile + " ...");

		HyperGraphPeer server = new HyperGraphPeer(new File(configFile));
		
		
		try
        {
            if (server.start("user", "pwd").get())
            {		
            	try
            	{
            		Thread.sleep(3000);
            	} catch (InterruptedException e){}

//			System.out.println("List of connected peers:");
//			for(RemotePeer peer : server.getConnectedPeers())
//			{
//				System.out.println("Connected peer: " + peer);
//			}
            	
            	HGPersistentHandle typeHandle = UUIDPersistentHandle.makeHandle("e917bda6-0932-4a66-9aeb-3fc84f04ce57");
//			server.registerType(typeHandle, User.class);
            	System.out.println("Types registered...");
            	/*
            	//server.setAtomInterests(new AtomPartCondition(new String[] {"part"}, "5", ComparisonOperator.LT));
            	server.setAtomInterests(new AnyAtomCondition());
            	
            	//server.setAtomInterests(new AtomPartCondition(new String[] {"part"}, "5", ComparisonOperator.LT));
            	server.updateNetworkProperties();
            	server.setAtomInterests(new AnyAtomCondition());
            	server.catchUp();
            	
            	HyperGraph graph = server.getHGDB();
            	for(int i=0;i<count;i++)
            	{
            		User user = new User(startId + i, "user " + Integer.toString(startId + i));
            		HGHandle handle = graph.add(user);
            		System.out.println("object added");
            		
            		User user1 = new User(startId + i, "new user " + Integer.toString(startId + i));
            		graph.replace(handle, user1);
            		System.out.println("object updated");
            		
            		//graph.remove(handle);
            		//System.out.println("object removed");
            		
            	}*/
            
            }else{
            	System.out.println("Can not start peer");
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
	}
}
