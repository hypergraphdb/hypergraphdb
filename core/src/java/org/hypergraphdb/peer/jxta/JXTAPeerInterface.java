package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.Structs.*;

import java.io.InputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.jxta.id.IDFactory;
import net.jxta.pipe.PipeID;
import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerNetwork;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Protocol;

/**
 * @author Cipri Costa
 *
 * Implements the PeerInterface interface and manages the communication with the other peers in the JXTA network.
 * Also manages resources like task allocation and threads.
 */

public class JXTAPeerInterface implements PeerInterface, JXTARequestHandler
{
    private String peerName = null;
	PipeAdvertisement pipeAdv = null;
	
	/**
	 * used to create the message
	 */
	private Protocol protocol = new Protocol();
	private HyperGraphPeer thisPeer = null;
	private JXTANetwork jxtaNetwork = new DefaultJXTANetwork();
	private MessageHandler messageHandler;
	
	
	private ExecutorService executorService;
	private JXTAServer jxtaServer = null;

	
	public boolean configure(Map<String, Object> configuration) 
	{
	    peerName = (String)getOptPart(configuration, "HGDBPeer", PeerConfig.PEER_NAME);
		return jxtaNetwork.configure(configuration);
	}
	
	public void stop()
	{
		if (jxtaNetwork != null)
		{
			jxtaNetwork.stop();
		}
		
		if (jxtaServer != null)
		{
			jxtaServer.stop();
		}
	}
	
	private void startNetwork(final ExecutorService executorService)
	{		
	    assert messageHandler != null : new 
	        NullPointerException("No message handler for PeerInterface " + this);
		PipeID pipeID = IDFactory.newPipeID(jxtaNetwork.getPeerGroup().getPeerGroupID());
		System.out.println("created pipe: " + pipeID.toString());
		pipeAdv = HGAdvertisementsFactory.newPipeAdvertisement(pipeID, peerName);
		
		jxtaNetwork.addOwnPipe(pipeID);
		jxtaNetwork.publishAdv(pipeAdv);
		jxtaNetwork.join(executorService);		
	}
	
	public void setMessageHandler(MessageHandler messageHandler)
	{
	    this.messageHandler = messageHandler;
	}
	
	public void run(final ExecutorService executorService) 
	{
		this.executorService = executorService;
		startNetwork(executorService);
		
		jxtaServer = new JXTAServer(this);
		if (jxtaServer.initialize(jxtaNetwork.getPeerGroup(), pipeAdv))
		{
			executorService.execute(jxtaServer);
		}
	}
	
	public void handleRequest(Socket socket)
	{
		executorService.execute(new ConnectionHandler(socket, executorService));
	}
	
	public HyperGraphPeer getThisPeer()
	{
	    return thisPeer;
	}
	
	public void setThisPeer(HyperGraphPeer thisPeer)
	{
	    this.thisPeer = thisPeer;
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
	
	public Future<Boolean> send(Object target, Object msg)
	{
	    PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
	    PeerRelatedActivity act = activityFactory.createActivity(); 
        act.setTarget(target);
        act.setMessage(msg);
        return executorService.submit(act);    
	}
	
	public void broadcast(Object msg)
	{
	    //
	    // TODO: we should replace this with a real broadcast. Unfortunately,
	    // JXTA doesn't have one. The following discussion is relevant:
	    // http://forums.java.net/jive/thread.jspa?messageID=324744&#324744
	    //
        PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
        PeerFilter peerFilter = newFilterActivity(null);
        peerFilter.filterTargets();
        Iterator<Object> it = peerFilter.iterator();

        // do startup tasks - filter peers and send messages
        while (it.hasNext())
        {
            Object target = it.next();
            PeerRelatedActivity act = activityFactory.createActivity(); 
            act.setTarget(target);
            act.setMessage(msg);
            executorService.submit(act);
        }	    
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

		@SuppressWarnings("unchecked")
		private void handleRequest(Socket socket, ExecutorService executorService) 
		{
		    InputStream in = null;
            try 
            {
            	in = socket.getInputStream();
            	try
            	{
            		final Message msg = new Message((Map<String, Object>)protocol.readMessage(in));            		
                    executorService.execute(new Runnable()
                    {
                        public void run() { messageHandler.handleMessage(msg); }
                    }
                    );            		
            	}
            	catch(Exception ex)
                {
            		// TODO: where are those messages reported? Do we simply send a 
            		// NotUnderstand response?
                	ex.printStackTrace();
                	return;
                }
            } 
            catch (Exception ie) 
            {
                ie.printStackTrace(System.err);
            }
            finally
            {
                if (in != null) try { in.close(); } catch (Throwable t) { t.printStackTrace(System.err); }
                try { socket.close(); } catch (Throwable t) { t.printStackTrace(System.err); }                                
            }
        }

		public void run() 
		{
			handleRequest(socket, executorService);
		}
	}

	public PeerNetwork getPeerNetwork()
	{
		return jxtaNetwork;
	}
}
