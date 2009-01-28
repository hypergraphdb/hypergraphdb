package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.HGDBOntology.ACTION;
import static org.hypergraphdb.peer.HGDBOntology.PERFORMATIVE;
import static org.hypergraphdb.peer.HGDBOntology.SEND_TASK_ID;
import static org.hypergraphdb.peer.Structs.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

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

/**
 * @author Cipri Costa
 *
 * Implements the PeerInterface interface and manages the communication with the other peers in the JXTA network.
 * Also manages resources like task allocation and threads.
 */

public class JXTAPeerInterface implements PeerInterface
{
	private Map<String, Object> config;
	PipeAdvertisement pipeAdv = null;
	
	/**
	 * used to create the message
	 */
	private Protocol protocol = new Protocol();
	
	private JXTANetwork jxtaNetwork = new DefaultJXTANetwork();

	private HashMap<Pair<Performative, String>, TaskFactory> taskFactories = new HashMap<Pair<Performative,String>, TaskFactory>();
	private HashMap<UUID, TaskActivity<?>> tasks = new HashMap<UUID, TaskActivity<?>>();
	private HGAtomPredicate atomInterests;
	
	public boolean configure(Map<String, Object> configuration) 
	{
		return jxtaNetwork.configure(getStruct(configuration, JXTAConfig.CONFIG_NAME));
	}
	
	private void startNetwork(final ExecutorService executorService)
	{
		String peerName = (String)getOptPart(config, "HGDBPeer", JXTAConfig.PEER_NAME);
		
		PipeID pipeID = IDFactory.newPipeID(jxtaNetwork.getPeerGroup().getPeerGroupID());
		System.out.println("created pipe: " + pipeID.toString());
		pipeAdv = HGAdvertisementsFactory.newPipeAdvertisement(pipeID, peerName);
		
		jxtaNetwork.addOwnPipe(pipeID);
		jxtaNetwork.publishAdv(pipeAdv);
		jxtaNetwork.join(executorService);		
	}
	
	public void run(final ExecutorService executorService) 
	{
		startNetwork(executorService);
		executorService.execute(new Runnable() { public void run() {
	        System.out.println("Starting ServerSocket");
	        JxtaServerSocket serverSocket = null;
	        
	        try 
	        {
	            serverSocket = new JxtaServerSocket(jxtaNetwork.getPeerGroup(), pipeAdv);
	            serverSocket.setSoTimeout(0);
	        } 
	        catch (IOException e) 
	        {
	            System.out.println("failed to create a server socket");
	            e.printStackTrace();
	        }
	        
	        //TODO implement a stop method
	        while (true) 
	        {
	            try 
	            {
	                System.out.println("Waiting for connections");
	                Socket socket = serverSocket.accept();
	                if (socket != null) 
	                {
	                    System.out.println("New socket connection accepted");
	                    executorService.execute(new ConnectionHandler(socket, executorService));
	                }
	            } 
	            catch (Exception e) 
	            {
	                e.printStackTrace();
	            }
	        }
		
		}});
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
	
	private class ConnectionHandler implements Runnable
	{
		private Socket socket;
		private ExecutorService executorService;
		
		public ConnectionHandler(Socket socket, ExecutorService executorService)
		{
			this.socket = socket;
			this.executorService = executorService;
		}

		private void handleRequest(Socket socket, ExecutorService executorService) 
		{
            try 
            {
            	System.out.println("JXTAPeerInterface: connection received");
            	
            	InputStream in = socket.getInputStream();
            	//OutputStream out = socket.getOutputStream();

                //get the data through the protocol
            	Object msg = null;
            	try
            	{
            		msg = protocol.readMessage(in);
            	}
            	catch(Exception ex)
                {
                	ex.printStackTrace();
                }
                System.out.println("received: " + msg.toString());
                if (tasks.containsKey(getPart(msg, SEND_TASK_ID)))
                {
                	tasks.get(getPart(msg, SEND_TASK_ID)).handleMessage(msg);
                }
                else
                {
                    Object x = getPart(msg, PERFORMATIVE); // variable needed because of Java 5 compiler bug                    
	                Pair<Performative, String> key = new Pair<Performative, String>(
	                		Performative.valueOf(x.toString()), 
	                		(String)getPart(msg, ACTION));
	                if (taskFactories.containsKey(key))
	                {
	                	TaskActivity<?> task = taskFactories.get(key).newTask(JXTAPeerInterface.this, msg);
	                	executorService.execute(task);
	                }
                }
                in.close();
                socket.close();                
                System.out.println("JXTAPeerInterface: connection closed");
            } 
            catch (Exception ie) 
            {
                ie.printStackTrace();
            }
        }

		public void run() 
		{
			handleRequest(socket, executorService);
		}
	}

	public void registerTask(UUID taskId, TaskActivity<?> task)
	{
		tasks.put(taskId, task);
	}


	public void execute(PeerRelatedActivity activity)
	{
		activity.run();
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
