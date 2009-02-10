package org.hypergraphdb.peer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;


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

	void send(Object networkTarget, Object msg);
	
	void registerTaskFactory(Performative performative, String action, TaskFactory convFactory);


	/**
	 * Register a task. All subsequent messages that have this task id are redirected to the 
	 * registered task. The task will have to decide if they are part of an existing conversation 
	 * or if a conversation has to be created for the message.
	 * 
	 * @param taskId
	 * @param task
	 */
	void registerTask(UUID taskId, TaskActivity<?> task);
	
	/**
	 * <p>
	 * Run the given activity. This method can be called only while the peer
	 * interface is currently running. The activity will be executed using
	 * the <code>ExecutorService</code> passed to the <code>run</code> method.
	 * </p>
	 * 
	 * @param activity The activity to execute. 
	 */
	void execute(PeerRelatedActivity activity);

	void setAtomInterests(HGAtomPredicate pred);
	HGAtomPredicate getAtomInterests();
}