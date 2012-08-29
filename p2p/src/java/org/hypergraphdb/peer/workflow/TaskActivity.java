/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;


import java.lang.reflect.InvocationTargetException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import mjson.Json;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.util.Pair;

/**
 * Base class for tasks. A task is an <code>AbstractActivity</code> that
 * manages certain conversations and uses the state changes in the conversations
 * as triggers for transactions in own state. Unlike Conversation activities,
 * messages are accepted even if the task is not in a state where a transition
 * is possible with that message. This class ensures implementors that those
 * messages are saved and will trigger transformations when (if ever) the task
 * enters an appropriate state.
 * 
 * The task is also registered against the <code>PeerInterface</code> and
 * routes all messages received through that interface to the appropriate
 * conversations.
 * 
 * @param <StateType>
 * 
 * @author Cipri Costa
 * 
 */
public abstract class TaskActivity<StateType> 
	extends AbstractActivity<StateType> implements ActivityStateListener
{
    private UUID taskId;
    private HyperGraphPeer thisPeer;

    /**
     * map of active conversations
     */
    private HashMap<UUID, Conversation<?>> conversations = new HashMap<UUID, Conversation<?>>();
    /**
     * each state has a queue of messages. When the task reaches the state, the
     * activity triggers a transaction.
     */
    private HashMap<StateType, LinkedBlockingQueue<AbstractActivity<?>>> activityQueues = new HashMap<StateType, LinkedBlockingQueue<AbstractActivity<?>>>();
    /**
     * Used to determine the queue of an activity based on the change in the
     * activity state
     * 
     * TODO: this map might be wrong - it assumes that there's only one task "interested state" per
     * conversation state, why is that assumption made?? (Boris)
     */
    private HashMap<Object, StateType> conversationQueueMaping = new HashMap<Object, StateType>();
    /**
     * map of registered transitions. Each transition also triggers a function
     * that is implemented in the child classes
     */
    private HashMap<Pair<StateType, Object>, Pair<StateType, Method>> transitions = new HashMap<Pair<StateType, Object>, Pair<StateType, Method>>();

    /**
     * Conversation states we are interested in
     */
    private HashSet<Object> conversationStates = new HashSet<Object>();

    public TaskActivity(HyperGraphPeer thisPeer, StateType start,
                        StateType end)
    {
        this(thisPeer, UUID.randomUUID(), start, end);
    }

    public TaskActivity(HyperGraphPeer thisPeer, UUID taskId,
                        StateType start, StateType end)
    {
        super(start, end);

        this.thisPeer = thisPeer;
        this.taskId = taskId;

//        thisPeer.getPeerInterface().registerTask(taskId, this);
    }
    
    protected void sendReply(Json originalMsg, Json reply)
    {
        getPeerInterface().send(originalMsg.at(Messages.REPLY_TO).getValue(), reply);
    }
    
    /**
     * Executes a transition
     * 
     * @param method
     * @param activity
     */
    @SuppressWarnings("unchecked")
    protected void handleActivity(Method method, AbstractActivity<?> activity)
    {
        try
        {
            StateType targetState = (StateType) method.invoke(this, activity);
            setState(targetState);

        }
        catch (IllegalArgumentException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InvocationTargetException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * <p>
     * Called when some sub-activity (e.g. a <code>Conversation</code>) state has changed. 
     * </p>
     * 
     * @param newState The sub-activity's new state.
     * @param activity The sub-activity itself.
     */
    public void stateChanged(Object newState, AbstractActivity<?> activity)
    {
        System.out.println("TaskActivity: conversation state changed to "
                           + newState + " while in " + getState());

        StateType interestedState = conversationQueueMaping.get(newState);

        LinkedBlockingQueue<AbstractActivity<?>> queue = activityQueues.get(interestedState);

        if (queue != null)
        {
            try
            {
                System.out.println("queueing message for " + interestedState);
                queue.put(activity);
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            System.out.println("can not find queue for " + interestedState);
        }
    }

    /**
     * <p>
     * Called by the peer interface when a message arrives for this task. Can be
     * overridden by derived <code>TaskActivity</code> implementations. The
     * default implementation delegates to the conversation identified by the 
     * <code>CONVERSATION_ID</code> attribute of the message (if any). If there's
     * no conversation associated with this message, an attempt to create a new one
     * is made by calling <code>createNewConversation</code> which can be overridden
     * also. If <code>createNewConversation</code> returns a non-null 
     * <code>Conversation</code> instance, the conversation is assigned an ID and
     * registered with this task.
     * </p>
     * 
     * <p>
     * So a task implementation can chose to implement <code>handleMessage</code> and
     * respond to messages directly on it own or it can just implement <code>createNewConversation</code>
     * to delegate the work to conversation implementations. Or it can have a combination
     * of both like the following pattern:
     * 
     * <pre><code>
     * public void handleMessage(Object msg)
     * {
     * 	  if msg should trigger a new conversation between this peer and
     *       the sending peer then
     *       
     *       super.handleMessage(msg);
     *    else
     *       respond to msg directly here...
     * }
     * 
     * public void createNewConversation(Object msg)
     * {
     *    // handleMessage determined that a new conversation must be started
     *    // so based on the content of the message, create and return
     *    // an appropriate conversation here.
     * }
     * </code></pre>
     * </p>
     * 
     * @param msg The message just received.
     * 
     */
    public void handleMessage(Json msg)
    {
        System.out.println("TaskActivity: handleMessage ");
        Conversation<?> conversation = null;
        UUID conversationId = Messages.fromJson(msg.at(Messages.CONVERSATION_ID));
        if (conversationId != null)
            conversation = conversations.get(conversationId);            
        if (conversation == null)
        {
            conversation = createNewConversation(msg);
            if (conversation != null)
            {
                conversation.setId(conversationId == null ? UUID.randomUUID() : conversationId);
                registerConversation(conversation, conversationId);
            }
        }
        if (conversation != null)
            conversation.handleIncomingMessage(msg);
    }

    /**
     * <p>
     * This method is called the first time the task is scheduled to run. It is 
     * an initialization method called after the task was constructed.
     * The default implementation of <code>startTask</code> does nothing.
     * </p>
     */
    protected void initiate()  {  }    

    protected void doRun()
    {
        compareAndSetState(null, startState);
    	
        while (!isStopped())
        {
            // get queue for current state
            LinkedBlockingQueue<AbstractActivity<?>> queue = activityQueues.get(getState());

            if (queue != null)
            {
                // wait for input on that
                AbstractActivity<?> activity = null;
                try
                {
                    System.out.println("wating on queue for " + getState());
                    activity = queue.take();
                }
                catch (InterruptedException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                if (activity != null)
                {
                    // get step info
                    Pair<StateType, Method> dest = transitions.get(new Pair<StateType, Object>(getState(),
                                                                                               activity.getState()));

                    if (compareAndSetState(getState(), dest.getFirst()))
                    {
                        handleActivity(dest.getSecond(), activity);
                    }
                }
            }
            else
            {
                // wait for state changes
                System.out.println("No queue found for " + getState() + " in "
                                   + this.getClass());

                try
                {
                    stateChangedLatch.await();
                }
                catch (InterruptedException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
//        thisPeer.getPeerInterface().unregisterTask(getTaskId());
    }

    protected void registerConversation(Conversation<?> conversation,
                                        UUID conversationId)
    {

        if (conversation != null)
        {
            // add handlers
            for (Object conversationState : conversationStates)
            {
                conversation.setStateListener(conversationState, this);
            }
            conversations.put(conversationId, conversation);
        }

    }

    /**
     * <p>
     * Create a new conversation based on the message content. This method
     * should be overridden by all tasks that need to create conversations between
     * two peers. The default implementation does not create any conversations.
     * </p>
     * 
     * @param msg The message just received from some peer in the context of this
     * task.
     * @return A newly created <code>Conversation</code>. The default implementation
     * returns <code>null</code>.
     */
    protected Conversation<?> createNewConversation(Json msg)
    {
        return null;
    }
    
    /**
     * <p>
     * Whenever the task is in state <code>fromState</code> and a conversation is in state 
     * <code>conversationState</code> the <code>functionName</code> function is called after 
     * the current state of the task is set to <code>toState</code>Working (this ensures 
     * that no other messages are processed while executing the function). 
     * </p>
     * @param fromState
     * @param conversationState
     * @param functionName
     * @param toState
     */
    protected void registerConversationHandler(StateType fromState,
                                               Object conversationState,
                                               String functionName,
                                               StateType toState)
    {
        // create a new input queue
        if (!activityQueues.containsKey(fromState))
        {
            LinkedBlockingQueue<AbstractActivity<?>> queue = new LinkedBlockingQueue<AbstractActivity<?>>();
            activityQueues.put(fromState, queue);
        }

        // add transition
        try
        {
            Method method = this.getClass().getMethod(functionName,
                                                      AbstractActivity.class);

            transitions.put(new Pair<StateType, Object>(fromState,
                                                        conversationState),
                            new Pair<StateType, Method>(toState, method));
        }
        catch (SecurityException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (NoSuchMethodException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        conversationQueueMaping.put(conversationState, fromState);
        // remember conversation state
        conversationStates.add(conversationState);
    }

    public UUID getTaskId()
    {
        return taskId;
    }

    public void setTaskId(UUID taskId)
    {
        this.taskId = taskId;
    }

    public HyperGraphPeer getThisPeer()
    {
        return thisPeer;
    }
    
    public PeerInterface getPeerInterface()
    {
        return thisPeer.getPeerInterface();
    }
}
