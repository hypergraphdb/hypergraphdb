package org.hypergraphdb.peer.workflow.replication;

import java.util.Iterator;
import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author ciprian.costa
 * A task that is used by a peer to publish its interests in the entire network
 * or by the peer interface to respond to queries about a peers interests.
 */
public class PublishInterestsTask extends TaskActivity<PublishInterestsTask.State>
{
	private HGAtomPredicate pred;
	
	protected enum State {Started, Done};

	/**
	 * Constructor called by current peer to publish his interests
	 * 
	 * @param peerInterface
	 * @param pred
	 */
	public PublishInterestsTask(HyperGraphPeer thisPeer, HGAtomPredicate pred)
	{
		super(thisPeer, State.Started, State.Done);
		this.pred = pred;
	}

	/**
	 *  Constructor called by peer interface -someone is requesting us to publish our interests
	 * @param peerInterface
	 * @param peer
	 * @param msg
	 */
	public PublishInterestsTask(HyperGraphPeer thisPeer, UUID taskId)
	{
		super(thisPeer, taskId, State.Started, State.Done);
		//start the conversation
		pred = thisPeer.getPeerInterface().getAtomInterests();
	}

	protected void initiate()
	{
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();
		PeerFilter peerFilter = getPeerInterface().newFilterActivity(null);

		peerFilter.filterTargets();
		Iterator<Object> it = peerFilter.iterator();
		while (it.hasNext())
		{
			Object target = it.next();
			sendMessage(activityFactory, target);
		}			
		setState(State.Done);
	}
	
	public void handleMessage(Object msg)
	{
		Object sendToTarget = getPart(msg, REPLY_TO);
		//send only to this peer
		sendMessage(getPeerInterface().newSendActivityFactory(), sendToTarget);
		setState(State.Done);
	}
	
	private void sendMessage(PeerRelatedActivityFactory activityFactory, Object target)
	{
		Object msg = createMessage(Performative.Inform, ATOM_INTEREST, getTaskId());
		combine(msg, struct(CONTENT, pred));
		
		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
		getPeerInterface().execute(activity);
	}
	
	public static class PublishInterestsFactory implements TaskFactory
	{
		public PublishInterestsFactory()
		{
		}
		public TaskActivity<?> newTask(HyperGraphPeer peerInterface, UUID taskId, Object msg)
		{
			return new PublishInterestsTask(peerInterface, taskId);
		}		
	}
}
