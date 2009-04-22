package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.Structs.*;

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
import org.hypergraphdb.peer.NetworkPeerPresenceListener;
import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;

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
	private HyperGraphPeer thisPeer = null;
	private DefaultJXTANetwork jxtaNetwork = new DefaultJXTANetwork();
	private MessageHandler messageHandler;
		
	private ExecutorService executorService;
	private JXTAServer jxtaServer = null;
	
	public void configure(Map<String, Object> configuration) 
	{
	    peerName = (String)getOptPart(configuration, "HGDBPeer", PeerConfig.PEER_NAME);
		if (!jxtaNetwork.configure(configuration))
		    throw new RuntimeException("Failed to configure JXTA network.");
	}
	
	public void stop()
	{
        if (jxtaServer != null)
        {
            jxtaServer.stop();
        }	    
		if (jxtaNetwork != null)
		{
			jxtaNetwork.stop();
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
	
	public void start() 
	{
		startNetwork(executorService);
		
		jxtaServer = new JXTAServer(this);
		if (jxtaServer.initialize(jxtaNetwork.getPeerGroup(), pipeAdv))
		{
			executorService.execute(jxtaServer);
		}
	}
	
	public void handleRequest(Socket socket)
	{
		executorService.execute(new ConnectionHandler(socket, messageHandler, executorService));
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
	
	public Future<Boolean> send(Object target, Message msg)
	{
	    PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
	    PeerRelatedActivity act = activityFactory.createActivity(); 
        act.setTarget(target);
        act.setMessage(msg);
        return executorService.submit(act);    
	}
	
	public void broadcast(Message msg)
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
	
	public void addPeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        jxtaNetwork.addPeerPresenceListener(listener);
    }
    
    public void removePeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        jxtaNetwork.removePeerPresenceListener(listener);
    }	
}