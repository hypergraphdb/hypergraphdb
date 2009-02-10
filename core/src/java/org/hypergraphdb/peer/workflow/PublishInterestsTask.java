package org.hypergraphdb.peer.workflow;

import java.util.Iterator;
import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
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
	private Object sendToTarget = null;
	
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
	public PublishInterestsTask(HyperGraphPeer thisPeer, Object msg)
	{
		super(thisPeer, (UUID)getPart(msg, SEND_TASK_ID), State.Started, State.Done);

		//start the conversation
		pred = thisPeer.getPeerInterface().getAtomInterests();
		sendToTarget = getPart(msg, REPLY_TO);
	}

	protected void startTask()
	{
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();
		if (sendToTarget != null)
		{
			//send only to this peer
			sendMessage(activityFactory, sendToTarget);
		}else{
			PeerFilter peerFilter = getPeerInterface().newFilterActivity(null);

			peerFilter.filterTargets();
			Iterator<Object> it = peerFilter.iterator();
			while (it.hasNext())
			{
				Object target = it.next();
				sendMessage(activityFactory, target);
			}			
		}
		
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
		public TaskActivity<?> newTask(HyperGraphPeer peerInterface, Object msg)
		{
			return new PublishInterestsTask(peerInterface, msg);
		}
		
	}
}
