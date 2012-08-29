/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;



import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.util.Pair;

/**
 * Superclass for conversation activities. The state transitions in a
 * conversation are triggered in two ways: by explicitly calling a method that
 * can "say" something or by receiving a message ("hearing" something)
 * 
 * The conversation will always remember the last message that was said or heard
 * as this is supposed to carry all the information that is needed about past
 * messages.
 * 
 * This class allows implementors to declaratively set what type of message to
 * except when the conversation is in a given state. If a message is received in
 * a state and there are no defined transitions for that state and the
 * performative of the message, a "do not understand" reply is sent.
 * 
 * @param <StateType>
 * 
 * @author Cipri Costa
 * @author Borislav Iordanov
 */
public class Conversation<StateType> extends AbstractActivity<StateType>
{
    // private static final UUID NULL_UUID = new UUID(0L, 0L);

    /**
     * The ID of this conversation.
     */
    private UUID id;

    /**
     * The task within which this conversation is taking place.
     */
    private TaskActivity<?> task;

    /**
     * The peer with which we are conversing.
     */
    private Object peer;

    /**
     * The last message sent or received.
     */
    private Json msg;

    private Map<Pair<StateType, Performative>, StateType> performativeTransitions = 
        new HashMap<Pair<StateType, Performative>, StateType>();

    public Conversation(TaskActivity<?> task, Object peer, StateType start,
            StateType end)
    {
        super(start, end);
        this.task = task;
        this.peer = peer;
    }

    protected void initiate()
    {
        // nothing to do...
    }
    
    protected void doRun()
    {
    }

    protected void registerPerformativeTransition(StateType fromState,
                                                  Performative performative,
                                                  StateType toState)
    {
        performativeTransitions.put(new Pair<StateType, Performative>(
                fromState, performative), toState);
    }

    public void handleIncomingMessage(Json msg)
    {
        compareAndSetState(null, startState);

        StateType state = getState();

        String x = msg.at(Messages.PERFORMATIVE).asString(); // variable needed because of
                                               // Java 5 compiler bug
        Pair<StateType, Performative> key = new Pair<StateType, Performative>(
                state, Performative.toConstant(x));
        StateType newState = performativeTransitions.get(key);

        if ((newState != null) && compareAndSetState(state, newState))
        {
            // new state set
            this.msg = msg;
        } else
        {
            // TODO say don't understand
        }
    }

    /**
     * <p>
     * Say something (i.e. send a message) to the peer with which this
     * conversation is taking place.
     * </p>
     * 
     * @param msg
     */
    protected void say(Json msg)
    {
        msg.set(Messages.CONVERSATION_ID, getId());
        task.getPeerInterface().send(peer, msg);
    }

    public Json getMessage()
    {
        return msg;
    }

    /*
     * public void setMessage(Object msg) { this.msg = msg; }
     */

    public UUID getId()
    {
        return id;
    }

    public void setId(UUID id)
    {
        this.id = id;
    }
}
