package org.hypergraphdb.peer.replication;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
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
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;

/**
 *
 * A task that performs the "server" side of the REMEMBER action. The task only manages a single 
 * conversation (with the client). The task is usually created when a call for proposal is received 
 * and decides in the startup phase whether to send a proposal or not.
 * 
 * @author Cipri Costa  
 */
public class RememberTaskServer extends TaskActivity<RememberTaskServer.State>
{
	protected enum State {BeforeStart, Started, HandleAccepted, HandleRejected, Done};
	
	private Timestamp last_version;
	private Timestamp current_version;
	private StorageService storage;
		
	public RememberTaskServer(HyperGraphPeer thisPeer, StorageService storage, UUID taskId)
	{
		super(thisPeer, taskId, State.Started, State.Done);
		this.storage = storage;
        registerConversationHandler(State.BeforeStart, ProposalConversation.State.Started, 
                                    "doPropose", State.Started);		
        registerConversationHandler(State.Started, ProposalConversation.State.Accepted, 
                                    "handleAccept", State.HandleAccepted);
        registerConversationHandler(State.Started, ProposalConversation.State.Rejected, 
                                    "handleReject", State.HandleRejected);		
	}

	@Override
    protected Conversation<?> createNewConversation(Message msg)
    { 	   
        return new ProposalConversation(this, getSender(msg));
    }
	
    public State doPropose(AbstractActivity<?> conversation)
    {
        ProposalConversation conv = (ProposalConversation)(AbstractActivity<ProposalConversation.State>)conversation;
        last_version = (Timestamp) getPart(conv.getMessage(), Messages.CONTENT, SLOT_LAST_VERSION);
        current_version = (Timestamp) getPart(conv.getMessage(), Messages.CONTENT, SLOT_CURRENT_VERSION);        
        Object reply = getReply(conv.getMessage());        
        conv.propose(reply);
        return State.Started;
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

		ProposalConversation conv = (ProposalConversation)(AbstractActivity<ProposalConversation.State>)conversation;
		Message msg = ((Conversation<?>)conversation).getMessage();		
		
		List<Object> handles = new ArrayList<Object>();
		
		Object peerId = getPeerInterface().getPeerNetwork().getPeerId(getPart(msg, Messages.REPLY_TO));//.getReplyTo());
		if (getThisPeer().getLog().registerRequest(peerId, last_version, current_version))
		{
			ArrayList<Object> contents = getPart(msg, Messages.CONTENT);
			
			for(Object content : contents)
			{
				StorageService.Operation operation = StorageService.Operation.valueOf((String)getPart(content, Messages.OPERATION));
				
				HGHandle handle = null;
				if (operation == StorageService.Operation.Create)
				{
					StorageGraph subgraph = (StorageGraph) getPart(content, Messages.CONTENT);
					handle = storage.addSubgraph(subgraph);
				}else if (operation == StorageService.Operation.Update){
				    StorageGraph subgraph = (StorageGraph) getPart(content, Messages.CONTENT);
					handle = storage.updateSubgraph(subgraph);
				}else if (operation == StorageService.Operation.Remove){
					handle = (HGPersistentHandle)getPart(content, Messages.CONTENT);
					storage.remove(handle);
				}else if (operation == StorageService.Operation.Copy){
				    StorageGraph subgraph = (StorageGraph) getPart(content, Messages.CONTENT);
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
			combine(reply, struct(Messages.CONTENT, handles));
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

		public TaskActivity<?> newTask(HyperGraphPeer peer, UUID taskId, Object msg)
		{
			return new RememberTaskServer(peer, getStorage(peer), taskId);
		}
	}
}
