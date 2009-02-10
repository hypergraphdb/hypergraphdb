package org.hypergraphdb.peer.workflow;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.Subgraph;
import org.hypergraphdb.peer.log.Timestamp;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;

/**
 * @author Cipri Costa
 *
 * A task that performs the "server" side of the REMEMBER action. The task only manages a single 
 * conversation (with the client). The task is usually created when a call for proposal is received 
 * and decides in the startup phase whether to send a proposal or not. 
 */
public class RememberTaskServer extends TaskActivity<RememberTaskServer.State>
{
	protected enum State {Started, HandleAccepted, HandleRejected, Done};
	
	private ProposalConversation conversation;
	private Timestamp last_version;
	private Timestamp current_version;
	private StorageService storage;
	
	public RememberTaskServer(HyperGraphPeer thisPeer, StorageService storage)
	{
		super(thisPeer, State.Started, State.Done);
		this.storage = storage;
		setState(State.Started);
	}
	
	public RememberTaskServer(HyperGraphPeer thisPeer, StorageService storage, Object msg)
	{
		super(thisPeer, (UUID)getPart(msg, SEND_TASK_ID), State.Started, State.Done);
		this.storage = storage;
		//start the conversation
		PeerRelatedActivity activity = (PeerRelatedActivity)getPeerInterface().newSendActivityFactory().createActivity();
		conversation = new ProposalConversation(activity, getPeerInterface(), msg);	
		
		last_version = (Timestamp) getPart(msg, CONTENT, SLOT_LAST_VERSION);//(Timestamp) ((Document)msg).get("last_version");
		current_version = (Timestamp) getPart(msg, CONTENT, SLOT_CURRENT_VERSION);//((Document)msg).get("curent_version");
	}

	protected void startTask()
	{
		//initialize transitions
		registerConversationHandler(State.Started, ProposalConversation.State.Accepted, "handleAccept", State.HandleAccepted);
		registerConversationHandler(State.Started, ProposalConversation.State.Rejected, "handleReject", State.HandleRejected);

		//do startup task (propose)
		Object reply = getReply(conversation.getMessage());
		//registerConversation(conversation, reply.getConversationId());
		registerConversation(conversation, (UUID)getPart(reply, CONVERSATION_ID));
		conversation.propose(reply);
	}
	
	/**
	 * called when a conversation enters the <code>Accepted</code> state while the task is in the <code>Started</code> state.
	 * 
	 * @param conversation
	 * @return
	 */
	public State handleAccept(AbstractActivity<?> conversation)
	{		
		System.out.println("RememberActivityServer: acccepting");

		ProposalConversation conv = (ProposalConversation)conversation;
		Object msg = ((Conversation<?>)conversation).getMessage();		
		
		List<Object> handles = new ArrayList<Object>();
		
		Object peerId = getPeerInterface().getPeerNetwork().getPeerId(getPart(msg, REPLY_TO));//.getReplyTo());
		if (getThisPeer().getLog().registerRequest(peerId, last_version, current_version))
		{
			ArrayList<Object> contents = getPart(msg, CONTENTS);
			
			for(Object content : contents)
			{
				StorageService.Operation operation = StorageService.Operation.valueOf((String)getPart(content, OPERATION));
				
				HGHandle handle = null;
				if (operation == StorageService.Operation.Create)
				{
					Subgraph subgraph = (Subgraph) getPart(content, CONTENT);
					handle = storage.addSubgraph(subgraph);
				}else if (operation == StorageService.Operation.Update){
					Subgraph subgraph = (Subgraph) getPart(content, CONTENT);
					handle = storage.updateSubgraph(subgraph);
				}else if (operation == StorageService.Operation.Remove){
					handle = (HGPersistentHandle)getPart(content, CONTENT);
					storage.remove(handle);
				}else if (operation == StorageService.Operation.Copy){
					Subgraph subgraph = (Subgraph) getPart(content, CONTENT);
					handle = storage.addOrReplaceSubgraph(subgraph);
				}
				handles.add(svalue(handle));
			}
/*			if (operation == StorageService.Operation.Create)
			{
				Subgraph subgraph = (Subgraph) getPart(msg, CONTENT);
				handle = peer.getStorage().addSubgraph(subgraph);
			}else if (operation == StorageService.Operation.Update){
				Subgraph subgraph = (Subgraph) getPart(msg, CONTENT);
				handle = peer.getStorage().updateSubgraph(subgraph);
			}else if (operation == StorageService.Operation.Remove){
				handle = (HGPersistentHandle)getPart(msg, CONTENT);
				peer.getStorage().remove(handle);
			}else if (operation == StorageService.Operation.Copy){
				Subgraph subgraph = (Subgraph) getPart(msg, CONTENT);
				handle = peer.getStorage().addOrReplaceSubgraph(subgraph);
			}
*/			
			getThisPeer().getLog().finishRequest(peerId, last_version, current_version);
			System.out.println("RememberActivityServer: remembered " + handles);
			
			Object reply = getReply(msg);
			combine(reply, struct(CONTENT, handles));
			conv.confirm(reply);
		}else{
			Object reply = getReply(msg);		
			
			conv.disconfirm(reply);
		}

		return State.Done;
	}
	
	public State handleReject(AbstractActivity<?> conversation)
	{
		//TODO why?
		
		return State.Done;
	}

	public static class RememberTaskServerFactory implements TaskFactory
	{
		private Map<HyperGraphPeer, StorageService> storage = 
		    new HashMap<HyperGraphPeer, StorageService>();
		
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
		
		public RememberTaskServerFactory()
		{
		}
		public TaskActivity<?> newTask(HyperGraphPeer peer, Object msg)
		{
			return new RememberTaskServer(peer, getStorage(peer), msg);
		}
	}
}
