package org.hypergraphdb.peer.workflow;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;


import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.query.HGQueryCondition;

import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author Cipri Costa
 * Starts a catch up action with a given peer or with all known peers. It will send CatchUp requests to other peers 
 * with the current state and the interests of the peer. The other peers will initiate conversations to help this peer come up to
 * date. 
 */
public class CatchUpTaskClient extends TaskActivity<CatchUpTaskClient.State>
{
	protected enum State {Started, Working, Done};

	private Object catchUpWith;
	private AtomicInteger count = new AtomicInteger(1);
	private HyperGraphPeer peer;
	
	public CatchUpTaskClient(PeerInterface peerInterface, Object catchUpWith, HyperGraphPeer peer)
	{
		super(peerInterface, State.Started, State.Done);
		
		this.catchUpWith = catchUpWith;
		this.peer = peer;
	}
	
	@Override
	protected void startTask()
	{
		System.out.println("Catching up...");
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

		if (catchUpWith != null)
		{
			sendMessage(activityFactory, catchUpWith);
			
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
		
		//to do ... wait for conversations
		setState(State.Done);

	}

	private void sendMessage(PeerRelatedActivityFactory activityFactory, Object target)
	{
		count.incrementAndGet();

		Object msg = createMessage(Performative.Request, CATCHUP, getTaskId());
		combine(msg, struct(CONTENT, 
				struct(SLOT_LAST_VERSION, peer.getLog().getLastFrom(target), 
						SLOT_INTEREST, getPeerInterface().getAtomInterests())));
				
		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
		getPeerInterface().execute(activity);
	}

	
	@Override
	public void stateChanged(Object newState, AbstractActivity<?> activity)
	{
		// TODO Auto-generated method stub

	}

	public State handleConfirm(AbstractActivity<?> fromActivity)
	{
		if (count.decrementAndGet() == 0) return State.Done;
		else return State.Started;
	}
	
	public State handleDisconfirm(AbstractActivity<?> fromActivity)
	{
		if (count.decrementAndGet() == 0) return State.Done;
		else return State.Started;
	}

}
