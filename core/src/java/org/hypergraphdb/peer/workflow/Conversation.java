package org.hypergraphdb.peer.workflow;

import java.util.HashMap;
import java.util.UUID;

import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.util.Pair;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;

/**
 * @author Cipri Costa
 *
 * Superclass for conversation activities. The state transitions in a conversation are triggered in two ways: by 
 * explicitly calling a method that can "say" something or by receiving a message ("hearing" something)
 *
 * The conversation will always remember the last message that was said or heard as this is supposed to carry 
 * all the information that is needed about past messages.
 * 
 * This class allows implementors to declaratively set what type of message to except when the conversation is 
 * in a given state. If a message is received in a state and there are no defined transitions for that state and
 * the performative of the message, a "do not understand" reply is sent.
 * 
 * @param <StateType>
 */
public class Conversation<StateType> extends AbstractActivity<StateType>
{
	private static final UUID NULL_UUID = new UUID(0L, 0L);
	
	private PeerRelatedActivity sendActivity;
	private PeerInterface peerInterface;
	private Object msg;

	private HashMap<Pair<StateType, Performative>, StateType> performativeTransitions = new HashMap<Pair<StateType,Performative>, StateType>();
	
	public Conversation(PeerRelatedActivity sendActivity, 
	                    PeerInterface peerInterface, 
	                    Object msg, 
	                    StateType start, 
	                    StateType end)
	{
		Object conversationId = getPart(msg, CONVERSATION_ID);
		if ((conversationId == null) || conversationId.equals(NULL_UUID))
		{
			combine(msg, struct(CONVERSATION_ID, UUID.randomUUID()));
		}
		this.sendActivity = sendActivity;
		this.msg = msg;
		this.peerInterface = peerInterface;
		
		this.sendActivity.setTarget(getPart(msg, REPLY_TO));
		
		setState(start);
	}


	protected void doRun()
	{
		
	}
	
	protected void registerPerformativeTransition(StateType fromState, Performative performative, StateType toState)
	{
		performativeTransitions.put(new Pair<StateType, Performative>(fromState, performative), toState);
	}

	public void handleIncomingMessage(Object msg)
	{
		StateType state = getState();
		
		Object x = getPart(msg, PERFORMATIVE); // variable needed because of Java 5 compiler bug
		Pair<StateType, Performative> key = new Pair<StateType, Performative>(state, Performative.valueOf(x.toString()));
		StateType newState = performativeTransitions.get(key);

		if ((newState != null) && compareAndSetState(state, newState))
		{
			//new state set
			this.msg = msg;
			stateChanged();
		}else{
			
			//TODO say don't understand
		}
	}

	protected void sendMessage()
	{
		sendActivity.setMessage(msg);
		peerInterface.execute(sendActivity);
	}
	
	public Object getMessage()
	{
		return msg;
	}

	public void setMessage(Object msg)
	{
		this.msg = msg;
	}
}
