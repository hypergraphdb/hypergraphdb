package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;

import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
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

	public ProposalConversation(PeerRelatedActivity sendActivity, PeerInterface peerInterface, Object msg)
	{
		super(sendActivity, peerInterface, msg, State.Started, State.Done);
		
		//serverside flow
		registerPerformativeTransition(State.Proposed, Performative.Accept, State.Accepted);

		//client side flow
		registerPerformativeTransition(State.Started, Performative.Proposal, State.Proposed);
		
		registerPerformativeTransition(State.Accepted, Performative.Confirm, State.Confirmed);
		registerPerformativeTransition(State.Accepted, Performative.Disconfirm, State.Disconfirmed);

	}

	/**
	 * called by server task when a proposal is to be sent
	 * @param msg
	 * @return
	 */
	public boolean propose(Object msg)
	{		
		if (compareAndSetState(State.Started, State.Proposed))
		{
			combine(msg, struct(PERFORMATIVE, Performative.Proposal));

			setMessage(msg);
			sendMessage();
			return true;
		}
		return false;
	}
	
	/**
	 * called by client task when accepting
	 * @param msg
	 */
	public boolean accept(Object msg)
	{
		if (compareAndSetState(State.Proposed, State.Accepted))
		{
			combine(msg, struct(PERFORMATIVE, Performative.Accept));

			setMessage(msg);
			sendMessage();
			
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
	public boolean confirm(Object msg)
	{
		System.out.println("ProposalConversation: confirm");
		if (compareAndSetState(State.Accepted, State.Confirmed))
		{
			combine(msg, struct(PERFORMATIVE, Performative.Confirm));

			setMessage(msg);
			sendMessage();

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
