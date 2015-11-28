/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;


import java.util.concurrent.atomic.AtomicInteger;

import mjson.Json;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.AbstractActivity;
import org.hypergraphdb.peer.workflow.TaskActivity;

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
	protected enum State {Started, Done};

	private Object catchUpWith;
	private AtomicInteger count = new AtomicInteger(1);
	private HyperGraphPeer thisPeer;
	
	public CatchUpTaskClient(HyperGraphPeer thisPeer, Object catchUpWith)
	{
		super(thisPeer, State.Started, State.Done);
		
		this.catchUpWith = catchUpWith;
		this.thisPeer = thisPeer;
	}
	
	@Override
	protected void initiate()
	{
		System.out.println("Catching up...");
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

		if (catchUpWith != null)
		{
			sendMessage(activityFactory, catchUpWith);
		}
		else
		{
			for (HGPeerIdentity peer : getThisPeer().getConnectedPeers())
			{
				Object target = getThisPeer().getNetworkTarget(peer);
				sendMessage(activityFactory, target);
			}				
		}		
		//to do ... wait for conversations
		setState(State.Done);
	}

	private void sendMessage(PeerRelatedActivityFactory activityFactory, Object target)
	{
		count.incrementAndGet();

		Json msg = createMessage(Performative.Request, CATCHUP, getTaskId());
		msg.set(Messages.CONTENT, 
				Json.object(SLOT_LAST_VERSION, thisPeer.getLog().getLastFrom(target), 
						SLOT_INTEREST, Replication.get(thisPeer).getAtomInterests()));
				
		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
//		getThisPeer(). .execute(activity);
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
