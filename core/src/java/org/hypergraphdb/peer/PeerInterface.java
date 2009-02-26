package org.hypergraphdb.peer;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 * This interface is implemented by classes that handle incoming and outgoing message
 * traffic for the peer. 
 * The interface has some factory methods that allow implementers to decide how to create
 * and allocate objects. 
 * TODO: manage threads from this object
 *
 * @author Cipri Costa
 */
public interface PeerInterface
{
    /**
     * <p>
     * There is only one <code>MessageHandler</code> for incoming message through
     * a given <code>PeerInterface</code> and this method sets it for this one.
     * </p>
     * @param message
     */
    void setMessageHandler(MessageHandler message);
    
	/**
	 * Because implementors can be of any type, the configuration is an Object, no constraints 
	 * to impose here as there is no common set of configuration properties.
	 * 
	 * @param configuration
	 * @return
	 */
	boolean configure(Map<String, Object> configuration);
	
	/**
	 * <p>
	 * Execute the message handling loop of this interface. This method is akin to a vanilla
	 * <code>run</code>, but with the additional constraint that a specific 
	 * <code>ExecutorService</code> must be used for the main message handling thread as
	 * well as for all activities triggered by this <code>PeerInterface</code>.
	 * </p>
	 * 
	 * @param executorService
	 */
	void run(ExecutorService executorService);

	/**
	 * <p>
	 * Stop the <code>PeerInterface</code> - no more messages are going to be
	 * received or sent.
	 * </p>
	 */
    void stop();
    
    /**
     * <p>
     * Return the <code>HyperGraphPeer</code> to which this <code>PeerInterface</code>
     * is bound.
     * </p>
     */
    HyperGraphPeer getThisPeer();
    
    /**
     * <p>
     * Internally used to initialize the <code>PeerInterface</code>, don't call in application code.
     * </p> 
     */
    void setThisPeer(HyperGraphPeer thisPeer);
    
	//factory methods to obtain activities that are specific to the peer implementation
	//TODO redesign
	PeerNetwork getPeerNetwork();
	PeerFilter newFilterActivity(PeerFilterEvaluator evaluator);
	PeerRelatedActivityFactory newSendActivityFactory();
	
	/**
	 * <p>
	 * Broadcast a message to all members of this peer's group.
	 * </p>
	 * 
	 * @param msg
	 */
	void broadcast(Object msg);

	Future<Boolean> send(Object networkTarget, Object msg);
}