package org.hypergraphdb.peer.workflow;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.PeerInterface;

/**
 * <p>
 * An <code>Activity</code> is some task that a peer is currently working one. An
 * activity is always currently in some state. 
 * </p>
 *
 * <p>
 * It is possible to directly extend this class and have complete control over 
 * the implementation of the activity simply by handling incoming messages and
 * taking some action at that point in time. This recommended either for very
 * simple P2P scenarios where not much state is maintained, or for very 
 * complicated ones that do not fit into the provided framework.  
 * </p>
 * 
 * <p>
 * For most cases however, it is probably best to implement the activity as an 
 * FSM (a Finite State Machine) that does from state to state based on incoming
 * messages and/or related sub-activities. In this case, the {@link FSMActivity} class
 * </p>
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

    protected HyperGraphPeer getThisPeer()
    {
        return thisPeer;
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

    /**
     * <p>A convenience method to send a message to a target peer.</p>
     * 
     * @param target The message recipient.
     * @param msg The message.
     */
    protected void send(Object target, Message msg)
    {
        getPeerInterface().send(target, msg);
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
    
    public Activity(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer; 
        this.id = UUID.randomUUID();
    }
    
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
    public abstract void handleMessage(Message message);
    
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
    
    public final UUID getId()
    {
        return id; 
    }
    
    public String toString()
    {
        return "activity[" + getId() + "]:" + getType();
    }
}