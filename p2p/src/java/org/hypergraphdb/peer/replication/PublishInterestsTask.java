/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;



import java.util.Iterator;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.Activity;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author ciprian.costa
 * A task that is used by a peer to publish its interests in the entire network
 * or by the peer interface to respond to queries about a peers interests.
 */
public class PublishInterestsTask extends Activity
{
	private HGAtomPredicate pred;
	
	/**
	 * Constructor called by current peer to publish his interests
	 * 
	 * @param peerInterface
	 * @param pred
	 */
	public PublishInterestsTask(HyperGraphPeer thisPeer, HGAtomPredicate pred)
	{
		super(thisPeer);
		this.pred = pred;
	}

	/**
	 *  Constructor called by peer interface -someone is requesting us to publish our interests
	 * @param peerInterface
	 * @param peer
	 * @param msg
	 */
	public PublishInterestsTask(HyperGraphPeer thisPeer, UUID id)
	{
		super(thisPeer, id);
		//start the conversation
		pred = Replication.get(thisPeer).getAtomInterests();
	}

	public void initiate()
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
		getState().setCompleted();
	}
	
	@Override
	public void handleMessage(Json msg)
	{
		Object sendToTarget = Messages.getSender(msg);
		//send only to this peer
		sendMessage(getPeerInterface().newSendActivityFactory(), sendToTarget);
        getState().setCompleted();
	}
	
	private void sendMessage(PeerRelatedActivityFactory activityFactory, Object target)
	{
		Json msg = Messages.createMessage(Performative.Inform, ATOM_INTEREST, getId());
		msg.set(Messages.CONTENT, pred);
		
		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
		getThisPeer().getExecutorService().submit(activity);
	}
}
