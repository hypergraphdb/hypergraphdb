/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * Represents an activity's current state. The set of possible activity states is open-ended
 * with a few predefined and recognized by the framework states. States are defined as 
 * symbolic constants through the <code>makeStateConstant</code>. A state constant is defined
 * simply by giving it a name. Only one state constant is bound a given name and this is 
 * ensured by this class. To obtain the state constant corresponding to a given name
 * use the <code>toStateConstant</code> method. To obtain a mutable state instance (usually
 * associated with an activity), use the <code>makeState</code>.     
 * </p>
 * 
 * <p>
 * State constants are implemented by the derived <code>WorkflowStateConstant</code> class 
 * which is intended to be used in Java <code>final</code> declarations only. Attempt to modify
 * a state constant will result in an exception.  
 * </p>
 * 
 * <p>
 * It is possible to track changes in a mutable state instance by registering a <code>StateListener</code>
 * with it. Whenever the state changes, all listeners are invoked in the order in which they were
 * added and all attempts to change the state are blocked while the listeners are being executed. So,
 * a state listener can safely assume that the state remains constant while the listener is executing.    
 * </p>
 * 
 * <p>
 * Predefined state constants must be used in implementations with the following defined semantics:
 * <ul>
 * <li><code>Limbo</code>: this is the initial state for newly created activities <b>before</b> they
 * have been initiated. An activity can have this state only at the peer that originally created it
 * and before it was actually initiated through the activity manager. It is impossible to explicitly
 * put an activity into this state.</li>
 * <li><code>Started</code>: indicates that the activity has been initiated and is currently running.</li>
 * <li><code>Completed</code>: indicates that an activity completed successfully, without exceptions.
 * Once an activity reaches this state, it is impossible to change it.</li> 
 * <li><code>Failed</code>: indicates that an activity failed with an exception.
 * Once an activity reaches this state, it is impossible to change it.</li> 
 * <li><code>Canceled</code>: indicates that an activity was explicitly canceled by the application.
 * Once an activity reaches this state, it is impossible to change it.</li> 
 * </ul>
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class WorkflowState
{
    private static Map<String, WorkflowStateConstant> constantPool = 
        new IdentityHashMap<String, WorkflowStateConstant>();
    
    //
    // Predefined activity states
    //
    public static final WorkflowStateConstant Limbo = makeStateConstant("Limbo");
    public static final WorkflowStateConstant Started = makeStateConstant("Started");
    public static final WorkflowStateConstant Completed = makeStateConstant("Completed");
    public static final WorkflowStateConstant Failed = makeStateConstant("Failed");
    public static final WorkflowStateConstant Canceled = makeStateConstant("Canceled");
        
    protected AtomicReference<String> name = new AtomicReference<String>("Limbo".intern());
    protected List<StateListener> listeners = null;

    // Cannot change the state to "boundary" values once it's already there.
    private String validateStateChange(WorkflowState s)
    {
        if (Limbo.name.get() == s.name.get()) // allowed only at construction time.
            return null;
        String c = name.get();        
        if (Completed.name.get() != c && Failed.name.get() != c && Canceled.name.get() != c)
            return c;
        else
            return null;
    }
    
    protected WorkflowState(String name)
    {
        this.name.set(name);        
        listeners = new ArrayList<StateListener>(1);
    }
        
    /**
     * <p>
     * Create a new custom state constant. The constants defined in this class
     * represent states recognized by the framework and implementation are 
     * required to use them following their semantics. Additional intermediary 
     * states that an activity implementation may need however should be first
     * declared as state constants through this method. State constants are 
     * maintained in a static constant pool where only one state constant with 
     * a given name may exist. This is unproblematic since states have only
     * symbolic value and independently developed activities can safely share
     * the same constant instances from the pool.
     * </p>
     */    
    public synchronized static WorkflowStateConstant makeStateConstant(String name)
    {
        String interned = name.intern();
        WorkflowStateConstant result = constantPool.get(interned);
        if (result == null)
        {
            result = new WorkflowStateConstant(interned);
            constantPool.put(interned, result);            
        }
        return result;
    }

    /**
     * <p>Return the state constant with the given name. The method will throw an exception
     * if there's no such constant.</p>
     */
    public synchronized static WorkflowStateConstant toStateConstant(String name)
    {
        WorkflowStateConstant c = constantPool.get(name.intern());
        if (c == null)
            throw new RuntimeException("Unknown state constant: " + name);
        return c;
    }
    
    /**
     * <p>Return a new state initialized from the passed in state constant.</p>
     */
    public static WorkflowState makeState(WorkflowStateConstant stateConstant)
    {
        return new WorkflowState(stateConstant.name.get());
    }
    
    /**
     * <p>Return a new state initialized to <code>Limbo</code>.</p>
     */
    public static WorkflowState makeState()
    {
        return makeState(Limbo);
    }
    
    /**
     * <p>Return the state constant corresponding to the current state.</p>
     */
    public WorkflowStateConstant getConst()
    {
        return constantPool.get(name.get());
    }
    
    /**
     * <p>
     * Moves to a new state (newState) if the current state is equal to a given
     * state (oldState)
     * </p>
     * 
     * @param oldState The presumed current state.
     * @param newState The new state. 
     * 
     * @return <code>true</code> if the change was made and <code>false</code> 
     * otherwise.
     * @throws an exception if this is a state constant. 
     */
    public boolean compareAndAssign(WorkflowStateConstant oldState, WorkflowStateConstant newState) 
    {
        if (newState == Limbo || oldState == Failed || 
            oldState == Completed || oldState == Canceled)
            throw new IllegalArgumentException("Invalid state change to " + 
                                               newState.name.get() + " from " + oldState.name.get());
        if (name.compareAndSet(oldState.name.get(), newState.name.get()))
        {
            for (StateListener l : listeners)
                l.stateChanged(this);
            return true;
        }
        else
            return false;
    }

    /**
     * <p>
     * Set a new state value. 
     * </p>
     * 
     * @param newState
     * @throws an exception if this is a state constant.
     */
    public void assign(WorkflowStateConstant newState)
    {
        String s = validateStateChange(newState);
        if (s == null)
            throw new IllegalArgumentException(
                "Invalid state change to " + newState.name.get() + " while current is " + toString());        
        if (name.compareAndSet(s, newState.name.get()))
            for (StateListener l : listeners)
                l.stateChanged(this);
        else
            throw new ConcurrentModificationException("Concurrent state change to " + 
                                                      newState.name.get() + " from " + name.get() +
                                                      " which changed recently from " + s);            
    }
    
    /**
     * <p>
     * Return a <code>Future</code> representing the task of this state reaching an expected
     * value. The future will complete as soon as the state changes to the <code>expected</code>
     * parameter. One can use such a <code>Future</code> to explicitly block and wait until
     * a certain state is reached. 
     * </p>
     * 
     * @param expected The expected state.
     */
    public Future<WorkflowStateConstant> getFuture(final WorkflowStateConstant expected)
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final StateListener L = new StateListener()
        {
            public void stateChanged(WorkflowState state)
            {
                if (expected == state.getConst())
                    latch.countDown();
            }            
        };
        addListener(L);
        return new Future<WorkflowStateConstant>()        
        {                     
            public boolean cancel(boolean arg0)
            {
                throw new UnsupportedOperationException();
            }
            
            public WorkflowStateConstant get() throws InterruptedException,
                    ExecutionException
            {
                latch.await();
                removeListener(L);
                return WorkflowState.this.getConst();
            }
            
            public WorkflowStateConstant get(long time, TimeUnit unit)
                    throws InterruptedException, ExecutionException,
                    TimeoutException
            {
                latch.await(time, unit);
                removeListener(L);
                return WorkflowState.this.getConst();
            }

            
            public boolean isCancelled()
            {
                return false;
            }
            
            public boolean isDone()
            {
                return latch.getCount() == 0;
            }                       
        };
    }
    
    /**
     * <p>Add a state change listener.</p>
     * 
     * @param l A non null listener.
     */
    public void addListener(StateListener l)
    {
        listeners.add(l);
    }

    /**
     * <p>Remove a previously added state change listener.</p>
     * 
     * @param l A non null listener.
     */
    public void removeListener(StateListener l)
    {
        listeners.remove(l);        
    }
    
    /**
     * <p>Return <code>true</code> iff this object represents a <em>Limbo</em>
     * state.</p>  
     */
    public boolean isInLimbo()
    {
        return name.get() == Limbo.name.get();
    }

    /**
     * <p>Return <code>true</code> iff this object represents a <em>Started</em>
     * state - that is, it is neither in limbo nor finished.</p>  
     */    
    public boolean isStarted()    
    {
        return !isInLimbo() && !isFinished();
    }
    
    /**
     * <p>Assign the <em>Started</em> state constant to this object.</p>
     */
    public void setStarted()
    {
        assign(Started);
    }
    
    /**
     * <p>Return <code>true</code> iff this object represents a <em>Completed</em>
     * state.</p>  
     */    
    public boolean isCompleted()
    {
        return name.get() == Completed.name.get();
    }
    
    /**
     * <p>Assign the <em>Completed</em> state constant to this object.</p>
     */    
    public void setCompleted()
    {
        assign(Completed);
    }
    
    /**
     * <p>Return <code>true</code> iff this object represents a <em>Failed</em>
     * state.</p>  
     */    
    public boolean isFailed()
    {
        return name.get() == Failed.name.get();
    }
    
    /**
     * <p>Assign the <em>Failed</em> state constant to this object.</p>
     */    
    public void setFailed()
    {
        assign(Failed);
    }
    
    /**
     * <p>Return <code>true</code> iff this object represents a <em>Canceled</em>
     * state.</p>  
     */    
    public boolean isCanceled()
    {
        return name.get() == Canceled.name.get();
    }
    
    /**
     * <p>Assign the <em>Canceled</em> state constant to this object.</p>
     */    
    public void setCanceled()
    {
        assign(Canceled);
    }
    
    /**
     * <p>Finished means it is either completed, failed or canceled.</p>
     */
    public boolean isFinished()
    {
        return isCompleted() || isFailed() || isCanceled();
    }    
    
    public int hashCode()
    {
        return name.get().hashCode();
    }
    
    public boolean equals(Object x)
    {
        if (x instanceof WorkflowState)
            return name.get() ==  ((WorkflowState)x).name.get();
        else
            return false;
    }
    
    public String toString() { return name.get(); }
}
