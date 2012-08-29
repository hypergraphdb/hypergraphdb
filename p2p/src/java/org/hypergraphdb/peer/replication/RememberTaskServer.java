/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;



import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.log.Timestamp;
import org.hypergraphdb.peer.workflow.AbstractActivity;
import org.hypergraphdb.peer.workflow.Conversation;
import org.hypergraphdb.peer.workflow.ProposalConversation;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.storage.StorageGraph;

import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Messages.*;

/**
 * 
 * A task that performs the "server" side of the REMEMBER action. The task only
 * manages a single conversation (with the client). The task is usually created
 * when a call for proposal is received and decides in the startup phase whether
 * to send a proposal or not.
 * 
 * @author Cipri Costa
 */
public class RememberTaskServer extends TaskActivity<RememberTaskServer.State>
{
	protected enum State
	{
		BeforeStart, Started, HandleAccepted, HandleRejected, Done
	};

	private Timestamp last_version;
	private Timestamp current_version;
	private StorageService storage;

	public RememberTaskServer(HyperGraphPeer thisPeer, StorageService storage,
			UUID taskId)
	{
		super(thisPeer, taskId, State.Started, State.Done);
		this.storage = storage;
		registerConversationHandler(State.BeforeStart,
				ProposalConversation.State.Started, "doPropose", State.Started);
		registerConversationHandler(State.Started,
				ProposalConversation.State.Accepted, "handleAccept",
				State.HandleAccepted);
		registerConversationHandler(State.Started,
				ProposalConversation.State.Rejected, "handleReject",
				State.HandleRejected);
	}

	@Override
	protected Conversation<?> createNewConversation(Json msg)
	{
		return new ProposalConversation(this, getSender(msg));
	}

	@SuppressWarnings("unchecked")
	public State doPropose(AbstractActivity<?> conversation)
	{
		ProposalConversation conv = (ProposalConversation) (AbstractActivity<ProposalConversation.State>) conversation;
		last_version = new Timestamp(conv.getMessage().at(Messages.CONTENT)
				.at(SLOT_LAST_VERSION).asInteger());
		current_version = new Timestamp(conv.getMessage().at(Messages.CONTENT)
				.at(SLOT_CURRENT_VERSION).asInteger());
		Json reply = getReply(conv.getMessage());
		conv.propose(reply);
		return State.Started;
	}

	/**
	 * called when a conversation enters the <code>Accepted</code> state while
	 * the task is in the <code>Started</code> state.
	 * 
	 * @param conversation
	 * @return
	 */
	public State handleAccept(AbstractActivity<?> conversation)
	{
		System.out.println("RememberActivityServer: acccepting");

		ProposalConversation conv = (ProposalConversation) (AbstractActivity<ProposalConversation.State>) conversation;
		Json msg = ((Conversation<?>) conversation).getMessage();

		Json handles = Json.array();

		HGPeerIdentity peerId = getPeerInterface().getThisPeer().getIdentity(
				Messages.getSender(msg));// .getReplyTo());
		if (getThisPeer().getLog().registerRequest(peerId, last_version,
				current_version))
		{
			Json contents = msg.at(Messages.CONTENT);

			for (Json content : contents.asJsonList())
			{
				StorageService.Operation operation = StorageService.Operation
						.valueOf(content.at(Messages.OPERATION).asString());

				HGHandle handle = null;
				if (operation == StorageService.Operation.Create)
				{
					StorageGraph subgraph = Messages.content(content);
					handle = storage.addSubgraph(subgraph);
				}
				else if (operation == StorageService.Operation.Update)
				{
					StorageGraph subgraph = Messages.content(content);					
					handle = storage.updateSubgraph(subgraph);
				}
				else if (operation == StorageService.Operation.Remove)
				{
					handle = Messages.content(content);					
					storage.remove(handle);
				}
				else if (operation == StorageService.Operation.Copy)
				{
					StorageGraph subgraph = Messages.content(content);
					handle = storage.addOrReplaceSubgraph(subgraph);
				}
				handles.add(handle);
			}
			/*
			 * if (operation == StorageService.Operation.Create) { Subgraph
			 * subgraph = (Subgraph) getPart(msg, CONTENT); handle =
			 * peer.getStorage().addSubgraph(subgraph); }else if (operation ==
			 * StorageService.Operation.Update){ Subgraph subgraph = (Subgraph)
			 * getPart(msg, CONTENT); handle =
			 * peer.getStorage().updateSubgraph(subgraph); }else if (operation
			 * == StorageService.Operation.Remove){ handle =
			 * (HGPersistentHandle)getPart(msg, CONTENT);
			 * peer.getStorage().remove(handle); }else if (operation ==
			 * StorageService.Operation.Copy){ Subgraph subgraph = (Subgraph)
			 * getPart(msg, CONTENT); handle =
			 * peer.getStorage().addOrReplaceSubgraph(subgraph); }
			 */
			getThisPeer().getLog().finishRequest(peerId, last_version,
					current_version);
			System.out.println("RememberActivityServer: remembered " + handles);

			Json reply = getReply(msg);
			reply.set(Messages.CONTENT, handles);
			conv.confirm(reply);
		}
		else
		{
			Object reply = getReply(msg);

			conv.disconfirm(reply);
		}

		return State.Done;
	}

	public State handleReject(AbstractActivity<?> conversation)
	{
		// TODO why?

		return State.Done;
	}

	public static class RememberTaskServerFactory implements TaskFactory
	{
		private Map<HyperGraphPeer, StorageService> storage = new HashMap<HyperGraphPeer, StorageService>();

		// TODO - how do we clean up this storage map when HyperGraphPeer
		// instances are closed???
		private synchronized StorageService getStorage(HyperGraphPeer peer)
		{
			StorageService ss = storage.get(peer);
			if (ss == null)
			{
				ss = new StorageService(peer);
				storage.put(peer, ss);
			}
			return ss;
		}

		public TaskActivity<?> newTask(HyperGraphPeer peer, UUID taskId,
				Object msg)
		{
			return new RememberTaskServer(peer, getStorage(peer), taskId);
		}
	}
}
