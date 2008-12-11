package org.hypergraphdb.peer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;


/**
 * @author Cipri Costa
 *
 * This interface is implemented by classes that handle incoming and outgoing message
 * traffic for the peer. 
 * The interface has some factory methods that allow implementers to decide how to create
 * and allocate objects. 
 * TODO: manage threads from this object
 *
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
	
	//factory methods to obtain activities that are specific to the peer implementation
	//TODO redesign
	PeerNetwork getPeerNetwork();
	PeerFilter newFilterActivity(PeerFilterEvaluator evaluator);
	PeerRelatedActivityFactory newSendActivityFactory();


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
	
	//TODO replace with an Executor approach
	void execute(PeerRelatedActivity activity);

	void setAtomInterests(HGAtomPredicate pred);
	HGAtomPredicate getAtomInterests();

}
