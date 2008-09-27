package org.hypergraphdb.peer.workflow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import static org.hypergraphdb.peer.HGDBOntology.*;

import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.peer.InterestEvaluator;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.Subgraph;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.log.LogEntry;
import org.hypergraphdb.peer.log.Timestamp;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import org.hypergraphdb.peer.protocol.Performative;

/**
 * @author Cipri Costa
 *
 * A task that performs the "client" side of the REMEMBER action. At the start of the task, it will send
 * "call for proposal" messages to peers using a <code>PeerFilter</code>. Any peer that decides to answer 
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
	private InterestEvaluator evaluator;
	private Log log;
	private List<LogEntry> entries;

	private List<Object> batch;
	
	//TODO replace. for now just assuming everyone is online 
	private AtomicInteger count = new AtomicInteger(1);
	PeerFilter peerFilter;
	private Object targetPeer;
	
	//private StorageService.Operation operation;
	
	public RememberTaskClient(PeerInterface peerInterface, Log log, Object targetPeer, List<Object> batch)
	{
		super(peerInterface, State.Started, State.Done);
		this.log = log;
		this.targetPeer = targetPeer;		
		this.batch = batch;
	}

	public RememberTaskClient(PeerInterface peerInterface, Object value, Log log, HyperGraph hg, HGPersistentHandle handle, StorageService.Operation operation)
	{
		super(peerInterface, State.Started, State.Done);
		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(handle, value, operation));
		this.log = log;
		
		evaluator = new InterestEvaluator(peerInterface, hg);
		
	}
	
	public RememberTaskClient(PeerInterface peerInterface, Object value, Log log, HGPersistentHandle handle, Object targetPeer, StorageService.Operation operation)
	{
		super(peerInterface, State.Started, State.Done);
		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(handle, value, operation));
		this.log = log;
		this.targetPeer = targetPeer;		
	}
	
	public RememberTaskClient(PeerInterface peerInterface, LogEntry entry, Object targetPeer, Log log)
	{
		super(peerInterface, State.Started, State.Done);
		
		entries = new ArrayList<LogEntry>();
		entries.add(entry);
		this.targetPeer = targetPeer;
		this.log = log;

		batch = new ArrayList<Object>();
		batch.add(new RememberEntity(null, null, entry.getOperation()));
	}

	protected void startTask()
	{		
		//initialize
		registerConversationHandler(State.Started, ProposalConversation.State.Proposed, "handleProposal", State.HandleProposal);
		
		registerConversationHandler(State.Accepted, ProposalConversation.State.Confirmed, "handleConfirm", State.HandleProposalResponse);
		registerConversationHandler(State.Accepted, ProposalConversation.State.Disconfirmed, "handleDisconfirm", State.HandleProposalResponse);		

		//do startup tasks - filter peers and send messages
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

		if (targetPeer == null)
		{
			peerFilter = getPeerInterface().newFilterActivity(evaluator);
		}

		if (entries == null)
		{
			entries = new ArrayList<LogEntry>();
			for(Object elem : batch)
			{
				RememberEntity entity = (RememberEntity) elem;
				HGPersistentHandle handle = entity.getHandle();
				
				if (handle == null) 
				{
					handle = UUIDPersistentHandle.makeHandle();
					entity.setHandle(handle);
				}
				LogEntry entry = log.createLogEntry(handle, entity.getAtom(), entity.getOperation());
				
				Iterator<Object> targets = getTargets(handle);			
				log.addEntry(entry, targets);
				
				entries.add(entry);
			}
		}
		
		if (peerFilter != null)
		{
			Iterator<Object> it = peerFilter.iterator();
			while (it.hasNext())
			{
				Object target = it.next();
				sendCallForProposal(target, activityFactory);
			}
		}else{
			sendCallForProposal(targetPeer, activityFactory);
		}
		
		if (count.decrementAndGet() == 0) setState(State.Done);
	}
	
	private Iterator<Object> getTargets(HGPersistentHandle handle)
	{
		if (targetPeer == null)
		{
			evaluator.setHandle(handle);
			peerFilter.filterTargets();
			
			return peerFilter.iterator();
		}else{
			ArrayList<Object> targets = new ArrayList<Object>();
			targets.add(targetPeer);
			
			return targets.iterator();
		}
	}

	private void sendCallForProposal(Object target, PeerRelatedActivityFactory activityFactory)
	{
		count.incrementAndGet();
		
		Object msg = createMessage(Performative.CallForProposal, REMEMBER_ACTION, getTaskId());
		
		LogEntry firstEntry = entries.get(0);
		LogEntry lastEntry = entries.get(entries.size() - 1);
		
		combine(msg, struct(
				CONTENT, struct(
						SLOT_LAST_VERSION, firstEntry.getLastTimestamp(getPeerInterface().getPeerNetwork().getPeerId(target)),
						SLOT_CURRENT_VERSION, lastEntry.getTimestamp()
					)
				)
		);

		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();
		activity.setTarget(target);
		activity.setMessage(msg);
		
		getPeerInterface().execute(activity);

	}
	/* (non-Javadoc)
	 * @see org.hypergraphdb.peer.workflow.TaskActivity#createNewConversation(org.hypergraphdb.peer.protocol.Message)
	 * 
	 * This function is called when the server started a conversation and the conversation has to be started on the cleint too.
	 */
	protected Conversation<?> createNewConversation(Object msg)
	{
		//TODO refactor
		PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();
		PeerRelatedActivity activity = (PeerRelatedActivity)activityFactory.createActivity();

		return new ProposalConversation(activity, getPeerInterface(), msg);
	}
	
	/**
	 * Called when one of the conversations enters the <code>Proposed</code> state while the task is in the 
	 * <code>Started</code> state. 
	 * 
	 * @param fromActivity
	 * @return
	 */
	public State handleProposal(AbstractActivity<?> fromActivity)
	{
		System.out.println("RememeberTaskClient: handleProposal");
		//there is a proposal, handle that
		ProposalConversation conversation = (ProposalConversation)fromActivity;
		
		//decide to accept or not ... for now just accept
		if (true)
		{
			Object reply = getReply(conversation.getMessage());
			
			ArrayList<Object> contents = new ArrayList<Object>();

			for(LogEntry entry : entries)
			{
				contents.add(
					struct(OPERATION, entry.getOperation(),
						CONTENT, (entry.getOperation() == StorageService.Operation.Remove) ? entry.getHandle() : object(entry.getData()))
				);
			}
			combine(reply, struct(CONTENTS, contents));
						
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
	public State handleConfirm(AbstractActivity<?> fromActivity)
	{
		Object msg = ((ProposalConversation)fromActivity).getMessage();	
		
		results = (ArrayList<HGHandle>)getPart(msg, CONTENT);

		Object peerId = getPeerInterface().getPeerNetwork().getPeerId(getPart(msg, REPLY_TO));
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
