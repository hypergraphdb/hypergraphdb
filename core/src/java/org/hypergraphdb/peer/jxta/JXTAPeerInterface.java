package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.HGDBOntology.ACTION;
import static org.hypergraphdb.peer.HGDBOntology.PERFORMATIVE;
import static org.hypergraphdb.peer.HGDBOntology.SEND_TASK_ID;
import static org.hypergraphdb.peer.Structs.getOptPart;
import static org.hypergraphdb.peer.Structs.getPart;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.UUID;

import net.jxta.id.IDFactory;
import net.jxta.pipe.PipeID;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaServerSocket;

import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerNetwork;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.protocol.Protocol;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.Pair;

import static org.hypergraphdb.peer.Structs.*;

/**
 * @author Cipri Costa
 *
 * Implements the PeerInterface interface and manages the communication with the other peers in the JXTA network.
 * Also manages resources like task allocation and threads.
 */

public class JXTAPeerInterface implements PeerInterface{
	private Object config;
	PipeAdvertisement pipeAdv = null;
	
	/**
	 * used to create the message
	 */
	private Protocol protocol = new Protocol();
	
	private JXTANetwork jxtaNetwork = new DefaultJXTANetwork();

	private HashMap<Pair<Performative, String>, TaskFactory> taskFactories = new HashMap<Pair<Performative,String>, TaskFactory>();
	private HashMap<UUID, TaskActivity<?>> tasks = new HashMap<UUID, TaskActivity<?>>();
	private HGAtomPredicate atomInterests;
	
	private boolean hasCustomInterface;
	
	public boolean configure(Object configuration, String user, String passwd) 
	{
		boolean result = true;
		
		System.out.println("JXTAPeerInterface: configure");

		//get the part we are interested in
		boolean hasTempDb = (Boolean)getOptPart(configuration, true, PeerConfig.HAS_TEMP_STORAGE);
		boolean hasLocalStorage = (Boolean)getPart(configuration, PeerConfig.HAS_LOCAL_STORAGE);

		hasCustomInterface = hasLocalStorage || hasTempDb;
		config = getPart(configuration, JXTAConfig.CONFIG_NAME);
		result = jxtaNetwork.init(config, user, passwd);

		if (result && hasCustomInterface)
		{
			String peerName = (String)getOptPart(config, "HGDBPeer", JXTAConfig.PEER_NAME);
			
			PipeID pipeID = IDFactory.newPipeID(jxtaNetwork.getPeerGroup().getPeerGroupID());
			System.out.println("created pipe: " + pipeID.toString());
			pipeAdv = HGAdvertisementsFactory.newPipeAdvertisement(pipeID, peerName);
			
			jxtaNetwork.addOwnPipe(pipeID);
			jxtaNetwork.publishAdv(pipeAdv);
			jxtaNetwork.start();
		}
		
		return result;		
	}

	public PeerFilter newFilterActivity(PeerFilterEvaluator evaluator)
	{
		JXTAPeerFilter result = new JXTAPeerFilter(jxtaNetwork.getAdvertisements());
		
		if (evaluator == null) evaluator = new DefaultPeerFilterEvaluator(null);
		result.setEvaluator(evaluator);
		
		return result;
	}


	public PeerRelatedActivityFactory newSendActivityFactory()
	{
		return new JXTASendActivityFactory(jxtaNetwork.getPeerGroup(), pipeAdv);
	}
	
	public void run() 
	{
		if (hasCustomInterface)
		{
	        System.out.println("Starting ServerSocket");
	        JxtaServerSocket serverSocket = null;
	        
	        try {
	        	serverSocket = new JxtaServerSocket(jxtaNetwork.getPeerGroup(), pipeAdv);
	            serverSocket.setSoTimeout(0);
	        } catch (IOException e) {
	            System.out.println("failed to create a server socket");
	            e.printStackTrace();
	        }
	        
	        //TODO implement a stop method
	        while (true) {
	            try {
	                System.out.println("Waiting for connections");
	                Socket socket = serverSocket.accept();
	                if (socket != null) {
	                    System.out.println("New socket connection accepted");
	                    Thread thread = new Thread(new ConnectionHandler(socket), "Connection Handler Thread");
	                    thread.start();
	                }
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
		}
	}

	private class ConnectionHandler implements Runnable{
		private Socket socket;
		
		public ConnectionHandler(Socket socket){
			this.socket = socket;
		}

		private void handleRequest(Socket socket) {
            try {
            	System.out.println("JXTAPeerInterface: connection received");
            	
            	InputStream in = socket.getInputStream();
            	//OutputStream out = socket.getOutputStream();

                //get the data through the protocol
            	Object msg = null;
            	try{
            		msg = protocol.readMessage(in);
            	}catch(Exception ex)
                {
                	ex.printStackTrace();
                }
                System.out.println("received: " + msg.toString());
                if (tasks.containsKey(getPart(msg, SEND_TASK_ID)))
                {
                	tasks.get(getPart(msg, SEND_TASK_ID)).handleMessage(msg);
                }else{
	                Pair<Performative, String> key = new Pair<Performative, String>(Performative.valueOf(getPart(msg, PERFORMATIVE).toString()), (String)getPart(msg, ACTION));
	                if (taskFactories.containsKey(key))
	                {
	                	TaskActivity<?> task = taskFactories.get(key).newTask(JXTAPeerInterface.this, msg);

	                	new Thread(task).start();
	                }
                }
                in.close();

                socket.close();
                
                System.out.println("JXTAPeerInterface: connection closed");
            } catch (Exception ie) {
                ie.printStackTrace();
            }
        }

		public void run() {
			handleRequest(socket);
		}
	}

	public void registerTask(UUID taskId, TaskActivity<?> task)
	{
		tasks.put(taskId, task);
	}


	public void execute(PeerRelatedActivity activity)
	{
		activity.run();
//		new Thread(activity).start();
	
	}


	public void registerTaskFactory(Performative performative, String action, TaskFactory taskFactory)
	{
		Pair<Performative, String> key = new Pair<Performative, String>(performative, action);
		
		taskFactories.put(key, taskFactory);
		
	}

	public void setAtomInterests(HGAtomPredicate pred)
	{
		atomInterests = pred;
		
	}

	public HGAtomPredicate getAtomInterests()
	{
		return atomInterests;
	}

	public PeerNetwork getPeerNetwork()
	{
		return jxtaNetwork;
	}


}
