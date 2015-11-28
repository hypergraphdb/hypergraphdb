/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;

import static org.hypergraphdb.peer.HGDBOntology.*;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.log.LogEntry;

import static org.hypergraphdb.peer.Messages.*;

import org.hypergraphdb.peer.workflow.AbstractActivity;
import org.hypergraphdb.peer.workflow.Conversation;
import org.hypergraphdb.peer.workflow.ProposalConversation;
import org.hypergraphdb.peer.workflow.TaskActivity;

/**
 * @author Cipri Costa
 *
 * A task that performs the "client" side of the REMEMBER action. 
 * At the start of the task, it will send "call for proposal" messages to peers 
 * using a <code>PeerFilter</code>. Any peer that decides to answer 
 * the call for proposal with a proposal will establish a conversation.
 * 
 * The task will only use <code>ProposalConversation</code> conversations. 
 * 
 * 
 */
public class RememberTaskClient extends TaskActivity<RememberTaskClient.State>
{
	protected enum State {Started, Accepted, HandleProposal, HandleProposalResponse, Done};

	private ArrayList<HGHandle> results;
	private Log log;
	private List<LogEntry> entries;

	private List<Object> batch;
	
	//TODO replace. for now just assuming everyone is online 
	private AtomicInteger count = new AtomicInteger(1);
	private Object targetPeer;
	
	//private StorageService.Operation operation;
	
	public RememberTaskClient(HyperGraphPeer thisPeer, Log log, Object targetPeer, List<Object> batch)
	{
		super(thisPeer, State.Started, State.Done);
		this.log = log;
		this.targetPeer = targetPeer;		
		this.batch = batch;
	}

	public RememberTaskClient(HyperGraphPeer thisPeer, Object value, Log log, HyperGraph hg, HGPersistentHandle handle, StorageService.Operation operation)
	{
		super(thisPeer, State.Started, State.Done);
		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(handle, value, operation));
		this.log = log;		
	}
	
	public RememberTaskClient(HyperGraphPeer thisPeer, Object value, Log log, HGPersistentHandle handle, Object targetPeer, StorageService.Operation operation)
	{
		super(thisPeer, State.Started, State.Done);
		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(handle, value, operation));
		this.log = log;
		this.targetPeer = targetPeer;		
	}
	
	public RememberTaskClient(HyperGraphPeer thisPeer, LogEntry entry, Object targetPeer, Log log)
	{
		super(thisPeer, State.Started, State.Done);
		
		entries = new ArrayList<LogEntry>();
		entries.add(entry);
		this.targetPeer = targetPeer;
		this.log = log;

		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(null, null, entry.getOperation()));
	}

	protected void initiate()
	{		
		//initialize
		registerConversationHandler(State.Started, ProposalConversation.State.Proposed, "handleProposal", State.HandleProposal);
		
		registerConversationHandler(State.Accepted, ProposalConversation.State.Confirmed, "handleConfirm", State.HandleProposalResponse);
		registerConversationHandler(State.Accepted, ProposalConversation.State.Disconfirmed, "handleDisconfirm", State.HandleProposalResponse);		

		//do startup tasks - filter peers and send messages
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

		if (entries == null)
		{
			entries = new ArrayList<LogEntry>();
			for(Object elem : batch)
			{
				RememberEntity entity = (RememberEntity) elem;
				HGPersistentHandle handle = entity.getHandle();
				
				if (handle == null) 
				{
					handle = this.getThisPeer().getGraph().getHandleFactory().makeHandle();
					entity.setHandle(handle);
				}
				LogEntry entry = log.createLogEntry(handle, entity.getAtom(), entity.getOperation());
				
				Iterator<Object> targets = getTargets(handle);			
				log.addEntry(entry, targets);
				
				entries.add(entry);
			}
		}
		
//		if (peerFilter != null)
//		{
//			for (HGPeerIdentity peer : getThisPeer().getConnectedPeers())
//			{
//				Object target = getThisPeer().getNetworkTarget(peer);
//				sendCallForProposal(target, activityFactory);
//			}
//		}else{
//			sendCallForProposal(targetPeer, activityFactory);
//		}
//		
		if (count.decrementAndGet() == 0) setState(State.Done);
	}
	
	private Iterator<Object> getTargets(HGPersistentHandle handle)
	{
//		if (targetPeer == null)
//		{
//			evaluator.setHandle(handle);
//			peerFilter.filterTargets();
//			
//			return peerFilter.iterator();
//		}else{
//			ArrayList<Object> targets = new ArrayList<Object>();
//			targets.add(targetPeer);
//			
//			return targets.iterator();
//		}
		return null;
	}

	private void sendCallForProposal(Object target, PeerRelatedActivityFactory activityFactory)
	{
		count.incrementAndGet();
		
		Json msg = createMessage(Performative.CallForProposal, REMEMBER_ACTION, getTaskId());
		
		LogEntry firstEntry = entries.get(0);
		LogEntry lastEntry = entries.get(entries.size() - 1);
		
		msg.set(
				Messages.CONTENT, Json.object(
						SLOT_LAST_VERSION, firstEntry.getLastTimestamp(getPeerInterface().getThisPeer().getIdentity(target)),
						SLOT_CURRENT_VERSION, lastEntry.getTimestamp()
					)
		);

		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
		getThisPeer().getExecutorService().submit(activity);

	}

	protected Conversation<?> createNewConversation(Json msg)
	{
		return new ProposalConversation(this, getSender(msg));
	}
	
	/**
	 * Called when one of the conversations enters the <code>Proposed</code> state while the task is in the 
	 * <code>Started</code> state. 
	 * 
	 * @param fromActivity
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public State handleProposal(AbstractActivity<?> fromActivity)
	{
		System.out.println("RememeberTaskClient: handleProposal");
		//there is a proposal, handle that
		ProposalConversation conversation = (ProposalConversation)(AbstractActivity<ProposalConversation.State>)fromActivity;
		
		//decide to accept or not ... for now just accept
		if (true)
		{
			Json reply = getReply(conversation.getMessage());
			
			ArrayList<Object> contents = new ArrayList<Object>();

			for(LogEntry entry : entries)
			{
				contents.add(
					Json.object(Messages.OPERATION, entry.getOperation(),
						Messages.CONTENT, (entry.getOperation() == StorageService.Operation.Remove) ? 
							entry.getHandle() : 
							SubgraphManager.encodeSubgraph(entry.getData()))
				);
			}
			reply.set(Messages.CONTENT, contents);
						
/*			if (rememberEntity.getOperation() == StorageService.Operation.Remove)
			{
				combine(reply, struct(CONTENT, rememberEntity.getHandle()));
			}else{
				combine(reply, struct(CONTENT, object(entry.getData())));
			}
*/			
			//set the conversation in the Accepted state
			conversation.accept(reply);
		}
		
		//return appropriate state
		return State.Accepted;
	}
	
	/**
	 * Called when one of the conversations enters the <code>Confirmed</code> state while the task is in the 
	 * <code>Accepted</code> state. 
	 * 
	 * @param fromActivity
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public State handleConfirm(AbstractActivity<?> fromActivity)
	{
		Json msg = ((ProposalConversation)(AbstractActivity<ProposalConversation.State>)fromActivity).getMessage();	
		
		results = Messages.content(msg);

		HGPeerIdentity peerId = getPeerInterface().getThisPeer().getIdentity(Messages.getSender(msg));
		log.confirmFromPeer(peerId, entries.get(entries.size() - 1).getTimestamp());
		
		if (count.decrementAndGet() == 0) return State.Done;
		else return State.Started;
	}
	
	public State handleDisconfirm(AbstractActivity<?> fromActivity)
	{
		//there is a disconfirm
		
		//back to get proposal ...
		return State.Started;
	}
	
	public HGHandle getResult()
	{
		System.out.println("RESULT: " + results.get(0));
		return results.get(0);
	}
	
	public List<HGHandle> getResults()
	{
		return results;
	}
	
	public static class RememberEntity
	{
		private HGPersistentHandle handle;
		private Object atom;
		private StorageService.Operation operation;
		
		public RememberEntity(HGPersistentHandle handle, Object atom, StorageService.Operation operation)
		{
			this.handle = handle;
			this.atom = atom;
			this.operation = operation;
		}

		public HGPersistentHandle getHandle()
		{
			return handle;
		}

		public void setHandle(HGPersistentHandle handle)
		{
			this.handle = handle;
		}

		public Object getAtom()
		{
			return atom;
		}

		public void setAtom(Object atom)
		{
			this.atom = atom;
		}

		public StorageService.Operation getOperation()
		{
			return operation;
		}

		public void setOperation(StorageService.Operation operation)
		{
			this.operation = operation;
		}
	}
}