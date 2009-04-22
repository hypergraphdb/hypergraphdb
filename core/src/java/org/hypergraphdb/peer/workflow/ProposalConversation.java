package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.Structs.*;

import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.protocol.Performative;

/**
 * @author Cipri Costa
 * implementation of the "Proposal" conversation. This conversation is typically triggered by a previous
 * "call for proposal" message. 
 * The flows of the conversation are:
 * Started 	
 * 	Proposed 	
 * 		Accepted
 * 			Confirmed
 * 			Disconfirmed
 * 		Rejected
 * 	
 * 
 */
public class ProposalConversation extends Conversation<ProposalConversation.State>
{
	public enum State {Started, Proposed, Accepted, Rejected, Confirmed, Disconfirmed, Done};

	public ProposalConversation(TaskActivity<?> task, Object peer)
	{
		super(task, peer, State.Started, State.Done);
		
		//serverside flow
		registerPerformativeTransition(State.Proposed, Performative.AcceptProposal, State.Accepted);

		//client side flow
		registerPerformativeTransition(State.Started, Performative.Propose, State.Proposed);		
		registerPerformativeTransition(State.Accepted, Performative.Confirm, State.Confirmed);
		registerPerformativeTransition(State.Accepted, Performative.Disconfirm, State.Disconfirmed);
	}

	/**
	 * Server-side behavior: send a proposal.
	 * @param msg
	 * @return
	 */
	public boolean propose(Message msg)
	{		
		if (compareAndSetState(State.Started, State.Proposed))
		{
			combine(msg, struct(Messages.PERFORMATIVE, Performative.Propose));

//			setMessage(msg);
			say(msg);
			return true;
		}
		return false;
	}
	
	/**
	 * called by client task when accepting
	 * @param msg
	 */
	public boolean accept(Message msg)
	{
		if (compareAndSetState(State.Proposed, State.Accepted))
		{
			combine(msg, struct(Messages.PERFORMATIVE, Performative.AcceptProposal));

//			setMessage(msg);
			say(msg);
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * called by client when rejecting
	 * @param msg
	 */
	public void reject(Object msg)
	{
		if (compareAndSetState(State.Proposed, State.Rejected))
		{
			//send acceptance
		}

	}
	
	/**
	 * called by server when confirming
	 * @param msg
	 */
	public boolean confirm(Message msg)
	{
		System.out.println("ProposalConversation: confirm");
		if (compareAndSetState(State.Accepted, State.Confirmed))
		{
			combine(msg, struct(Messages.PERFORMATIVE, Performative.Confirm));

//			setMessage(msg);
			say(msg);

			return true;
		}
		return false;
	}
	
	/**
	 * called by server when disconfirming
	 * @param msg
	 */
	public void disconfirm(Object msg)
	{
		if (compareAndSetState(State.Accepted, State.Disconfirmed))
		{
			//send acceptance
		}
	}
	
	
}
