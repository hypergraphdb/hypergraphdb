/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;



import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hypergraphdb.peer.Messages.*;
import mjson.Json;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * An <code>Activity</code> is some task that a peer is currently working one. An
 * activity is always currently in some state. 
 * </p>
 *
 * <p>
 * It is possible to directly extend this class and have complete control over 
 * the implementation of the activity simply by handling incoming messages and
 * taking some action at that point in time. This is recommended either for very
 * simple P2P scenarios where not much state is maintained, or for very 
 * complicated ones that do not fit into the provided framework.  
 * </p>
 * 
 * <p>
 * For most cases however, it is probably best to implement the activity as an 
 * FSM (a Finite State Machine) that does from state to state based on incoming
 * messages and/or related sub-activities. In this case, the {@link FSMActivity} class
 * must be extended instead.</p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class Activity
{
    /**
     * A queue of action for this activity to be execute in a sequential manner.
     * This queue is managed by the ActivityManager
     */
    volatile BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

    /**
     * Stores a timestamp of when the last action from the queue was executed. This
     * helps the ActivityManager scheduling be more fair.
     */
    volatile long lastActionTimestamp = 0;
    
    /**
     * The Future associated with an already initiated activity. Initialized
     * by the ActivityManager when this activity gets initiated.
     */
    volatile ActivityManager.ActivityFuture future;
    
    /**
     * This activity's ID.
     */
    private UUID id = null;
    
    /**
     * The HyperGraph peer for the activity;
     */
    private HyperGraphPeer thisPeer;
    
    /**
     * The current state of the activity
     */
    private WorkflowState state = WorkflowState.makeState();

    /**
     * <p>Return the <code>HyperGraphPeer</code> where this activity executes.</p>
     */
    public HyperGraphPeer getThisPeer()
    {
        return thisPeer;
    }
    
    /**
     * <p>Set the {@link HyperGraphPeer} context of this activity.</p>
     */
    void setThisPeer(HyperGraphPeer thisPeer)
    {
    	this.thisPeer = thisPeer;
    }
    
    /**
     * <p>Return the <code>PeerInterface</code> instance for communicating
     * with other peers. This is a convenience method that is equivalent to 
     * <code>getThisPeer().getPeerInterface()</code>.
     * </p>
     */
    protected PeerInterface getPeerInterface()
    {
        return thisPeer.getPeerInterface();
    }

    protected Json createMessage(Performative performative, Object content)
    {
        Json msg = Messages.createMessage(performative, this);
        Activity parent = thisPeer.getActivityManager().getParent(this);
        if (parent != null)
            msg.set(PARENT_SCOPE, parent.getId()).set(PARENT_TYPE, parent.getType());
        msg.set(CONTENT, content);
        return msg;
    }
    
    /**
     * <p>A convenience method to send a message to a target peer.</p>
     * 
     * @param target The message recipient.
     * @param msg The message.
     */
    protected void send(HGPeerIdentity target, Json msg)
    {
        Object networkTarget = thisPeer.getNetworkTarget(target);
        if (networkTarget == null)
            throw new RuntimeException("Unknown network target for peer " + 
                                       target + " - perhaps it dropped from the network?");
        try
        {
            if (!getPeerInterface().send(networkTarget, msg).get())
                throw new RuntimeException("Failed to send msg '" + msg + "' to " + target);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    protected Future<Boolean> post(HGPeerIdentity target, Json msg)
    {
        Object networkTarget = thisPeer.getNetworkTarget(target);
        if (networkTarget == null)
            throw new RuntimeException("Unknown network target for peer " + 
                                       target + " - perhaps it dropped from the network?");
        try
        {
            return getPeerInterface().send(networkTarget, msg);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }        
    }
    
    /**
     * <p>A convenience method to send a message to a target peer.</p>
     * 
     * @param target The message recipient - could be a <code>HGPeerIdentity</code>
     * or a transport-dependent network target.
     * @param msg The message.
     */
    protected void send(Object target, Json msg)
    {
        if (target instanceof HGPeerIdentity)
            send((HGPeerIdentity)target, msg);
        else
            getPeerInterface().send(target, msg);
    }
    
    protected Future<Boolean> post(Object target, Json msg)
    {
        if (target instanceof HGPeerIdentity)
            return post((HGPeerIdentity)target, msg);
        else
            return getPeerInterface().send(target, msg);
    }
    
    protected Future<Boolean> reply(Json msg, Performative perf, Object content)
    {
        return post(getSender(msg), getReply(msg, perf, content));
    }
    
    /**
     * <p>
     * Add an action to the action queue to be scheduled for execution some time
     * in the future.
     * </p>
     * 
     * @param action The action in the form of a <code>Runnable</code> object. 
     * If <code>null</code>, it will be ignored.
     * @throws InterruptedException If the current thread is interrupted during
     * the put into the action queue(which is a blocking queue).
     */
    protected void addAction(Runnable action) throws InterruptedException
    {
        if (action == null)
            return;
        queue.put(action);
    }
    
    /**
     * <p>Default constructor - thisPeer, ID etc must be set separately.</p>
     */
    Activity()
    {    	
    	this.id = UUID.randomUUID();
    }
    
    /**
     * <p>
     * Constructor for brand new activities - a random UUID is generated to
     * identify the newly constructed activity.
     * </p>
     * 
     * @param thisPeer
     */
    public Activity(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer; 
        this.id = UUID.randomUUID();
    }
    
    /**
     * <p>
     * Construct an new <code>Activity</code> from a given UUID. Use this 
     * constructor for new activities if you have your own means of UUID 
     * generation or for an existing activity to be instantiated locally.  
     * </p>
     * 
     * @param thisPeer
     * @param id
     */
    public Activity(HyperGraphPeer thisPeer, UUID id)
    {
        this.thisPeer = thisPeer; 
        this.id = id;
    }

    /**
     * <p>
     * Called by the framework to initiate a new activity. This method is only invoked
     * at the peer initiating the activity. Once an activity has been initiated, its state
     * changes to <code>Started</code>.   
     * </p> 
     */
    public abstract void initiate();

    /**
     * <p>
     * Handle an incoming that was identified as belonging to this activity. 
     * </p>
     * 
     * @param message The full message.
     */
    public abstract void handleMessage(Json message);
    
    /**
     * <p>Return this activity's workflow state.</p>
     */
    public final WorkflowState getState()
    {
        return state;
    }

    /**
     * <p>
     * Return the <code>Future</code> object representing the completion
     * of this activity.
     * </p>
     */
    public final Future<ActivityResult> getFuture()
    {
        return future;
    }
    
    /**
     * <p>
     * Return the type name of this activity. By the default to fully-qualified 
     * class name is returned. This method can be overridden by sub-classes to 
     * provide a short and/or more human-readable type name. 
     * </p>
     */
    public String getType()
    {
        return this.getClass().getName();
    }
    
    /**
     * Return the UUID of this activity.
     */
    public final UUID getId()
    {
        return id; 
    }
    
    void setId(UUID id)
    {
    	this.id = id;
    }
    
    public String toString()
    {
        return "activity[" + getId() + "]:" + getType();
    }
    
    public int hashCode()
    {
        return HGUtils.hashIt(getId());
    }
    
    public boolean equals(Object x)
    {
        if (! (x instanceof Activity))
            return false;
        else
            return HGUtils.eq(getId(), ((Activity)x).getId());
    }
}
