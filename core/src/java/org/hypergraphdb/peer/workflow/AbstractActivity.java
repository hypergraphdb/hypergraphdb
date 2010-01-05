/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Cipri Costa
 * 
 * This is the superclass of all the work flow like classes. The implementors of
 * this class provide a number of states and functions that move the object from
 * a state to another (usually enums, but not mandatory). Like in the case of an
 * finite state machine, the current state and the external signal will decide
 * which is the next state of the object.
 * 
 * Implementors can expose two special states (not mandatory): start and end.
 * The start state is initialized when the activity is started, while reaching
 * the end state will trigger the termination of the run method of the activity.
 * 
 * @param <StateType>
 */
public abstract class AbstractActivity<StateType> implements Runnable
{
    /**
     * The current state of the activity
     */
    private AtomicReference<StateType> state = new AtomicReference<StateType>();
    
    /**
     * Other objects can choose to be informed when the state of the activity
     * changes
     */
    private HashMap<StateType, ArrayList<ActivityStateListener>> stateListeners = new HashMap<StateType, ArrayList<ActivityStateListener>>();

    protected final StateType startState;
    protected final StateType endState;

    private CountDownLatch latch;
    protected CountDownLatch stateChangedLatch;

    public AbstractActivity(StateType start, StateType end)
    {
        this.startState = start;
        this.endState = end;
    }

    /**
     * <p>
     * Called by the framework to initiate a new activity. This method is only invoked
     * at the peer initiating the activity. Once an activity has been initiated, its state
     * changes to <code>start</code>.   
     * </p> 
     */
    protected abstract void initiate();
    
    /**
     * Overriden by implementors to do the actual work
     */
    protected abstract void doRun();
    
    public void run()
    {
        latch = new CountDownLatch(1);
        stateChangedLatch = new CountDownLatch(1);
        doRun();
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * returns true if the end state was set and is the current state
     * 
     * @return
     */
    protected boolean isStopped()
    {
        return endState.equals(getState());
    }

    protected void afterStateChanged(StateType newValue)
    {
        callStateListeners();
        if (latch != null && endState.equals(newValue))
        {
            System.out.println("latch released");
            latch.countDown();
        }

        if (stateChangedLatch != null)
        {
            stateChangedLatch.countDown();
            stateChangedLatch = new CountDownLatch(1);
        }
    }

    /**
     * Moves to a new state (newState) if the current state is equal to a given
     * state (oldState)
     * 
     * @param oldState
     * @param newState
     * @return
     */
    protected boolean compareAndSetState(StateType oldState, StateType newState)
    {
        if (state.compareAndSet(oldState, newState))
        {
            afterStateChanged(newState);
            return true;
        }
        else
            return false;
    }

    protected StateType getState()
    {
        return state.get();
    }

    protected void setState(StateType newValue)
    {
        System.out.println("AbstractActivity: changing state from "
                           + state.get() + " to " + newValue);
        state.set(newValue);
        afterStateChanged(newValue);
    }

    @SuppressWarnings("unchecked")
    public void setStateListener(Object state, ActivityStateListener listener)
    {
        ArrayList<ActivityStateListener> list = stateListeners.get(state);
        if (list == null)
        {
            list = new ArrayList<ActivityStateListener>();
            stateListeners.put((StateType) state, list);
        }

        list.add(listener);
    }

    /**
     * Calls the listeners. This function is not called from this class because
     * implementors need to decide when they are ready to report the state
     * change to interested objects.
     */
    private void callStateListeners()
    {
        StateType newState = state.get();
        ArrayList<ActivityStateListener> list = stateListeners.get(newState);
        if (list != null)
        {
            for (ActivityStateListener listener : list)
            {
                listener.stateChanged(newState, this);
            }
        }
    }
 
    /**
     * <p>Return the object indicating a starting state for this activity.</p> 
     */
    public StateType getStartState()
    {
        return startState;
    }

    /**
     * <p>Return the object indicating an ending state for this activity.</p> 
     */    
    public StateType getEndState()
    {
        return endState;
    }
}
