/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;



import static org.hypergraphdb.peer.workflow.WorkflowState.*;
import static org.hypergraphdb.peer.Messages.*;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import mjson.Json;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.serializer.HGPeerJsonFactory;
import org.hypergraphdb.util.HGUtils;


/**
 * 
 * <p>
 * The <code>ActivityManager</code> manages all activities currently in effect within
 * a given peer.   
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ActivityManager implements MessageHandler
{
    private HyperGraphPeer thisPeer;
    
    private Map<String, ActivityType> activityTypes = 
        Collections.synchronizedMap(new HashMap<String, ActivityType>());
    
    private Map<UUID, Activity> activities = 
        Collections.synchronizedMap(new HashMap<UUID, Activity>());
    
    private Map<Activity, Activity> parents = 
        Collections.synchronizedMap(new HashMap<Activity, Activity>());
 
    //
    // Scheduling and action queues. Each activity has an associated queue of actions
    // that are executed in a FIFO fashion. Actions are added as messages related to the
    // activity are received or state transitions are triggered. 
    //
    // The scheduling algorithm uses a global priority queue of activity queues. At each
    // cycle, the next activity queue is removed from the global queue and its head action
    // is submitted for execution. Upon completion of that action, the activity queue is
    // inserted back into the global priority queue. This ensures that no two actions on a single
    // activity are performed concurrently.
    //    
    final BlockingQueue<Activity> globalQueue = 
        new PriorityBlockingQueue<Activity>(10, new Comparator<Activity>()
        {
            // Priorities are compared as follows: if one of the two activities
            // has a Future.get blocking for it to complete, but not the other,
            // that activity has a higher priority whenever its action queue is 
            // NOT empty. Otherwise, a weight based
            // on the amount of time elapsed since an action was taken on an activity
            // and the size of its own action queue is calculated and whoever has the
            // bigger weight has priority.
            public int compare(Activity left, Activity right)
            {
                if (left.future.isWaitedOn())
                {
                    if (!right.future.isWaitedOn() && !left.queue.isEmpty())
                        return -1;
                }
                else if (right.future.isWaitedOn() && !right.queue.isEmpty())
                    return 1;
                long st = System.currentTimeMillis();
                long diff = (st-right.lastActionTimestamp)*(1 + right.queue.size())-
                            (st-left.lastActionTimestamp)*(1 + left.queue.size());
                // can't simply cast diff to int cause it'll screw up the sign
                return diff > 0 ? 1 : diff < 0 ? -1 : 0;
            }
        }
    );    
    
    private class ActivitySchedulingThread extends Thread
    {
        volatile boolean schedulerRunning = false;
        
        public ActivitySchedulingThread() { super("HGDB Peer Scheduler"); }
        public void run()
        {
        	int reportEmpty = 0;
            for (schedulerRunning = true; schedulerRunning; )
            {
                try
                {
                    // Need to 'poll' instead of 'take' so that the scheduler can
                    // be gracefully stopped.
                    Activity a = globalQueue.poll(1, TimeUnit.SECONDS);
                    if (a == null)
                    {
                    	if (reportEmpty >= 50)
                    	{
                    		//System.out.println("ActivityManager Global Queue is empty");
                    		reportEmpty = 0;
                    	}
                    	reportEmpty++;                    	
                        continue;
                    }
                    else if (!a.queue.isEmpty() && !a.getState().isFinished())
                    {
                        Runnable r = a.queue.take();
                        thisPeer.getExecutorService().execute(r);
                    }
                    else 
                    {
                        if (globalQueue.isEmpty()) 
                        {
                            Thread.sleep(100); // really? sleep here? that much?
                        }
                        a.lastActionTimestamp = System.currentTimeMillis();
                        if (!a.getState().isFinished())
                            globalQueue.put(a);
                    }
                }
                catch (InterruptedException ex) { break; }
            }
            schedulerRunning = false;
        }
    }
    
    private ActivitySchedulingThread schedulerThread = null;
    
    private void handleActivityException(Activity activity, Throwable exception, Json msg)
    {
        activity.future.result.exception = exception;
        // TODO: what if already in ending state? is that possible?
        activity.getState().assign(WorkflowState.Failed); 
        exception.printStackTrace(System.err);
        if (msg != null)
            thisPeer.getPeerInterface().send(getSender(msg), 
                                             getReply(msg, 
                                                      Performative.Failure, 
                                                      HGUtils.printStackTrace(exception)));
    }
    
    private Activity findRootActivity(Activity a)
    {
        Activity root = a; 
        for (Activity tmp = parents.get(root); tmp != null; tmp = parents.get(root))
            root = tmp;
        return root;
    }
    
    private void notUnderstood(final Json msg, final String explanation)
    {
        try 
        { 
        	Json reply = Messages.getReply(msg) 
                                   .set(PERFORMATIVE, Performative.NotUnderstood.toString())
                                   .set(CONTENT, msg)
                                   .set(WHY_NOT_UNDERSTOOD, explanation);
            thisPeer.getPeerInterface().send(getSender(msg), reply);
            //System.out.println("Sending not understood on " + msg + " because " + exlanation);
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err); // TODO: logging API? so global 'exception handler'
        }
    }
    
    private Runnable makeTransitionAction(final ActivityType type,
                                          final Activity parentActivity, 
                                          final Activity activity)
    {
        return new Runnable() {
            public void run()
            {
                Activity rootActivity = findRootActivity(parentActivity);                
                try 
                {
                	Json.attachFactory(HGPeerJsonFactory.getInstance().setHyperGraph(thisPeer.getGraph()));                	
                    Transition transition = 
                        type.getTransitionMap().getTransition(parentActivity.getState().getConst(), 
                                                              activity, 
                                                              activity.getState().getConst());
                    
                    if (transition == null)
                        return;                    
                    WorkflowStateConstant result = transition.apply(parentActivity, activity);
                    if (result != null) // is there a state change?
                        parentActivity.getState().assign(result);
                }
                catch (Throwable t)
                {
                    handleActivityException(parentActivity, t, null);
                }
                finally
                {
                	Json.dettachFactory();
                    parentActivity.lastActionTimestamp = System.currentTimeMillis();                    
                    try
                    {
                        globalQueue.put(rootActivity);
                    }
                    catch (InterruptedException ex)
                    {
                        // nothing really we can do about this
                        handleActivityException(rootActivity, ex, null);
                    }
                }
            }
         };  
    }    
    
    private Runnable makeTransitionAction(final ActivityType type,
                                          final FSMActivity activity, 
                                          final Json msg)
    {
        return new Runnable() {
            public void run()
            {
                Activity rootActivity = findRootActivity(activity);                
                try 
                {
                	Json.attachFactory(HGPeerJsonFactory.getInstance().setHyperGraph(thisPeer.getGraph()));
                    Transition transition = 
                        type.getTransitionMap().getTransition(activity.getState().getConst(), 
                                                              msg);
                    if (transition == null)
                    {
                        Performative perf = Performative.toConstant(msg.at(PERFORMATIVE).asString());
                        if (perf == Performative.Failure)
                            activity.onPeerFailure(msg);
                        else if (perf == Performative.NotUnderstood)
                            activity.onPeerNotUnderstand(msg);
                        else
                            notUnderstood(msg, " no state transition defined for this performative.");
                    }
                    else
                    {
                        //System.out.println("Running transition " + transition + " on msg " + msg);
//                        Thread.currentThread().setContextClassLoader(thisPeer.getGraph().getConfig().getClassLoader());
                        WorkflowStateConstant result = transition.apply(activity, msg);
//                        System.out.println("Transition finished with " + result);
                        if (result != null)
                            activity.getState().assign(result);
                    }                                        
                }
                catch (Throwable t)
                {                    
                    handleActivityException(activity, t, msg);                    
                }
                finally
                {
                	Json.dettachFactory();
                    // Reschedule root only if it hasn't failed - in particular when
                    // a sub-activity fails with an exception in the above, the root
                    // will be rescheduled.
                    activity.lastActionTimestamp = System.currentTimeMillis();                    
                    try
                    {
                        if (!rootActivity.getState().isFinished())
                            globalQueue.put(rootActivity);
                    }
                    catch (InterruptedException ex)
                    {
                        // nothing really we can do about this
                        handleActivityException(rootActivity, ex, null);
                    }                    
                }
            }
         };  
    }    
    
    private Runnable makeMessageHandleAction(final Activity activity, 
                                             final Json msg)
    {
        return new Runnable() {
            public void run()
            {
                Activity rootActivity = findRootActivity(activity);                
                try 
                {
                	Json.attachFactory(HGPeerJsonFactory.getInstance().setHyperGraph(thisPeer.getGraph()));
                    activity.handleMessage(msg);                   
                }
                catch (Throwable t)
                {
                    handleActivityException(activity, t, msg);
                }
                finally
                {
                	Json.dettachFactory();
                    // Reschedule root only if it hasn't failed - in particular when
                    // a sub-activity fails with an exception in the above, the root
                    // will be rescheduled.
                    activity.lastActionTimestamp = System.currentTimeMillis();                    
                    try
                    {
                        if (!rootActivity.getState().isFinished())
                            globalQueue.put(rootActivity);
                    }
                    catch (InterruptedException ex)
                    {
                        // nothing really we can do about this
                        handleActivityException(rootActivity, ex, null);
                    }                     
                }
            }
         };  
    }
    
    private void readTransitionMap(Class<? extends Activity> activityClass, TransitionMap map)
    {
        for (Method m : activityClass.getMethods())
        {
            FromState aFromState = m.getAnnotation(FromState.class);            
            OnMessage onMessage = m.getAnnotation(OnMessage.class);           
            AtActivity atActivity = m.getAnnotation(AtActivity.class);
            OnActivityState onState = m.getAnnotation(OnActivityState.class);
            if (atActivity != null && onState == null || onState != null && atActivity == null)
                throw new RuntimeException("Both OnStateActivity and AtActivity annotations need " +
                   "to be specified for method " + m + " in class " + activityClass.getName() + 
                   " or neither.");
             
            if (aFromState == null)
            {
                if (onMessage != null || atActivity != null || onState != null)
                    throw new RuntimeException("A transition method needs to be annotated with " +
                                " with a FromState annotation.");
                else
                    continue;
            }
            else if (onMessage == null && atActivity == null)
                    throw new RuntimeException("A transition method needs to be annotated either " +
                                " with an OnMessage or both AtActivity and OnActivityState annotations.");
                
            Map<String, String> msgAttrs = null;
            if (onMessage != null)
            {
                msgAttrs = new HashMap<String, String>();
                msgAttrs.put("performative", onMessage.performative());
            }
            
            Transition t = new MethodCallTransition(m);
            for (String from : aFromState.value())
            {
                WorkflowStateConstant fromState = WorkflowState.toStateConstant(from);
                if (msgAttrs != null)
                    map.setTransition(fromState, 
                                      msgAttrs, 
                                      t);
                if (atActivity != null)
                {
                    for (String to : onState.value())                        
                        map.setTransition(fromState, 
                                          atActivity.value(), 
                                          WorkflowState.toStateConstant(to),
                                          t);
                }
            }
        }
    }
    
    public ActivityManager(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer;
    }
    
    public void start()
    {
        schedulerThread = new ActivitySchedulingThread();
        // TODO - is this the right thing to do, using the class loader defined in the
        // HyperGraph instance? Classes are loaded either by the type systems within
        // the core of HGDB, or in the P2P Structs classes that does serialization/deserialization
        // of beans.
//        if (thisPeer.getGraph() != null)
//        	schedulerThread.setContextClassLoader(thisPeer.getGraph().getConfig().getClassLoader());
        schedulerThread.start();
    }

    public void stop()
    {        
        if (schedulerThread == null)
            return;
        schedulerThread.schedulerRunning = false;
        try 
        {
            if (schedulerThread.isAlive())
                schedulerThread.join();
        }
        catch (InterruptedException ex)
        {            
        }
        finally
        {
            schedulerThread = null;
        }
    }
    
    /**
     * <p>
     * Clear all internal data structures such as registered activities,
     * queues of pending actions etc. This method should never be called
     * while the scheduler is currently running.
     * </p>
     */
    public void clear()
    {
        this.activities.clear();
        this.activityTypes.clear();
        this.parents.clear();
        this.globalQueue.clear();
    }
    
    /**
     * <p>
     * Clear all activity-related data structures. This method should 
     * never be called while the scheduler is currently running. Registered
     * activity types remain so there's no need to re-register and the start
     * method could be called again.
     * </p>
     */    
    public void clearActivities()
    {
        this.activities.clear();
        this.parents.clear();
        this.globalQueue.clear();        
    }
    
    /**
     * <p>
     * Retrieve an {@link Activity} by its UUID.
     * </p>
     */
    public Activity getActivity(UUID id)
    {
        return activities.get(id);
    }
    
    /**
     * <p>
     * A simplified version of <code>registerActivityType</code> in which the
     * type name is taken to be the fully qualified classname of the 
     * <code>activityClass</code> parameter and a <code>DefaultActivityFactory</code>
     * instance is going to be used to create new activities of that type.
     * </p>
     * 
     * @param activityClass The class implementing the activity. 
     */
    public void registerActivityType(Class<? extends Activity> activityClass)
    {
        registerActivityType(activityClass.getName(), 
                             activityClass, 
                             new DefaultActivityFactory(activityClass));
    }

    /**
     * <p>
     * Register an activity type with an associated factory. The factory will be used
     * to construct new activity instances based on incoming message. 
     * </p>
     * 
     * @param activityClass The class implementing the activity. 
     * @param factory The activity factory associated with this type. 
     */
    public void registerActivityType(Class<? extends Activity> activityClass, 
                                     ActivityFactory factory)
    {
        registerActivityType(activityClass.getName(), activityClass, factory);
    }

    /**
     * <p>
     * Register an activity type with the specified non-default type name.
     * </p>
     * @param type The type name.
     * @param activityClass The class that implements the activity.
     */
    public void registerActivityType(String type, 
                                     Class<? extends Activity> activityClass) 
    {
        registerActivityType(type, activityClass, new DefaultActivityFactory(activityClass));
    }
    
    /**
     * <p>
     * Register an activity type with the specified non-default type name and 
     * factory.
     * </p>
     * @param type The type name.
     * @param activityClass The class that implements the activity.
     * @param factory The activity factory associated with this type. 
     */
    public void registerActivityType(String type, 
                                     Class<? extends Activity> activityClass, 
                                     ActivityFactory factory)
    {
        if (activityTypes.containsKey(type))
            throw new IllegalArgumentException("Activity type '" + type + "' already registered.");
        
        ActivityType activityType = new ActivityType(type, factory);
        readTransitionMap(activityClass, activityType.getTransitionMap());
        activityTypes.put(type, activityType);
    }
    
    public Future<ActivityResult> initiateActivity(final Activity activity)
    {
        return initiateActivity(activity, null, null);
    }
    
    public Future<ActivityResult> initiateActivity(final Activity activity, 
                                                   final ActivityListener listener)
    {
        return initiateActivity(activity, null, listener);
    }
    
    /**
     * <p>
     * Initiate a new activity. 
     * </p>
     * 
     * @param activity
     * @param parentActivity
     * @param listener
     * @return
     */
    public Future<ActivityResult> initiateActivity(final Activity activity,
                                                   final Activity parentActivity,
                                                   final ActivityListener listener)
    {
    	try
    	{
    		Json.attachFactory(HGPeerJsonFactory.getInstance().setHyperGraph(thisPeer.getGraph()));
    		activity.setThisPeer(thisPeer);
	        ActivityFuture future = insertNewActivity(activity, parentActivity, listener);
	        activity.getState().compareAndAssign(Limbo, Started);        
	        activity.initiate();                       
	        return future;
    	}
    	finally
    	{
    		Json.dettachFactory();
    	}
    }
    
    private ActivityFuture insertNewActivity(final Activity activity, 
                                             final Activity parentActivity,
                                             final ActivityListener listener)
    {
        synchronized (activities)
        {
            if (activities.containsKey(activity.getId()))
                throw new RuntimeException("Activity " + activity + 
                                           " with ID " + activity.getId() + " has already been initiated.");

            activities.put(activity.getId(), activity);
        }
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final ActivityFuture future = new ActivityFuture(activity, completionLatch);        
        activity.future = future;        
        activity.getState().addListener(new StateListener() {
            public void stateChanged(WorkflowState state)
            {
//                System.out.println("Activity state change " + activity + " to " + state.toString());
                if (state.isFinished())
                {
//                    System.out.println("Activity " + activity + " is finished.");
                    completionLatch.countDown();
                    if (listener != null)
                        try { listener.activityFinished(future.get()); }
                        catch (Throwable t) { t.printStackTrace(System.err); /* this shouldn't ever happen! */}
                    // TODO: is it really correct to remove the activity from the map at this point?
                    // what if we get a message about this activity afterwards? Maybe activities should 
                    // be designed in such a way that the message content should be such that either
                    // it's ok to create a brand new activity from it, or it should be refused with a
                    // "not-understood" performative.
                        
                    // The question is: is it correct to remove activities when they reach a 
                    // finished state. It is unlikely that the globalQueue.remove below will
                    // actually remove the activity from the global queue because this listener
                    // is probably called within a thread executing an activity "action". And
                    // the activity is in fact not in the global queue while one of its actions
                    // is being executed.
                    globalQueue.remove(activity);
                    activities.remove(activity.getId());
                    parents.remove(activity);
                }
            }
        });
        if (parentActivity != null)
        {
            // Serialize actions of all children within parent action queue so that states
            // changes are serialized and handled in order.
            activity.queue = parentActivity.queue; 
            parents.put(activity, parentActivity);
            activity.getState().addListener(new StateListener() { 
                public void stateChanged(WorkflowState state)
                {
                    ActivityType pt = activityTypes.get(parentActivity.getType()); 
                    parentActivity.queue.add(makeTransitionAction(pt, parentActivity, activity));
                }
            });
        }
        else try
        {
            globalQueue.put(activity);
        }
        catch (InterruptedException ex)
        {
            // nothing really we can do about this
            handleActivityException(activity, ex, null);
        }
        return future;
    }
    
    public void handleMessage(final Json msg)
    {
        //System.out.println("Received message " + msg);    	
        UUID activityId = Messages.fromJson(msg.at(Messages.CONVERSATION_ID));
        if (activityId == null)
        {
            notUnderstood(msg, " missing conversation-id in message");
            return;            
        }
        Activity activity = activities.get(activityId);
        ActivityType type = null;        
        // TODO: how is this synchronized in case several peers send something
        // about this same activity?
        if (activity == null)
        {
            Activity parentActivity = null;            
            UUID parentId = Messages.fromJson(msg.at(Messages.PARENT_SCOPE));
            if (parentId != null)
            {
                parentActivity = activities.get(parentId);
                if (parentActivity == null)
                {
                    // Or should we just ignore the fact that we don't know about
                    // the parent activity here? It seems ok that sub-activities
                    // could be b/w a different group of peers than the parent
                    // activities
//                    notUnderstood(msg, " unkown parent activity " + parentId);
//                    return;
                }
            }
            type = activityTypes.get(msg.at(Messages.ACTIVITY_TYPE).asString());
            if (type == null)
            {
                notUnderstood(msg, " unkown activity type '" + type + "'");
                return;                
            } 
            activity = type.getFactory().make(thisPeer, activityId, msg);
            activity.setThisPeer(thisPeer);
            activity.setId(activityId);
            insertNewActivity(activity, parentActivity, null);
            //System.out.println("inserted new activity in queue " + activity.getId());            
            activity.getState().compareAndAssign(Limbo, Started);            
        }
        else
        {
            //System.out.println("Msg for existing activity " + activity.getId() + " at state " + activity.getState());        	
            type = activityTypes.get(activity.getType());
            if (type == null)                
                handleActivityException(activity, new NullPointerException("no local activity type found with name " + activity.getType()), msg);
        }
        try
        {
            //System.out.println("Adding transition action to " + activity.queue.size() + " others in " + activity + " on msg  "+ msg);        	
            if (activity instanceof FSMActivity)
                activity.queue.put(makeTransitionAction(type, (FSMActivity)activity, msg));
            else
                activity.queue.put(makeMessageHandleAction(activity, msg));
        }
        catch (InterruptedException ex)
        {
            // Main message handling thread is being interrupted, we are probably shutting the application
            // down, so nothing much to do...
            handleActivityException(activity, ex, msg);
        }        
    }
    
    public Activity getParent(Activity a)
    {
        return parents.get(a);
    }
    
    //-------------------------------------------------------------------------
    // 
    //
    class ActivityFuture implements Future<ActivityResult>
    {
        ActivityResult result;
        CountDownLatch latch;
        AtomicInteger waiting = new AtomicInteger(0);
        
        boolean isWaitedOn()
        {
            return waiting.get() > 0;
        }
        
        public ActivityFuture(Activity activity, CountDownLatch latch)
        {
            result = new ActivityResult(activity);
            this.latch = latch;
        }
        
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException();
        }

        public ActivityResult get() 
            throws InterruptedException, ExecutionException
        {
            waiting.incrementAndGet();
            try
            {
                latch.await();
            }
            catch (InterruptedException ex)
            {
                waiting.decrementAndGet();
                throw ex;
            }
            return result;
        }

        public ActivityResult get(long timeout, TimeUnit unit) 
            throws InterruptedException, ExecutionException, TimeoutException
        {            
            waiting.incrementAndGet();
            try
            {
                if (!latch.await(timeout, unit))
                {
                    waiting.decrementAndGet();
                    return null;
                }
                else
                    return result;
            }
            catch (InterruptedException ex)
            {
                waiting.decrementAndGet();
                throw ex;
            }            
        }

        public boolean isCancelled()
        {
            return result.getActivity().getState().isCanceled();
        }

        public boolean isDone()
        {
            return result.getActivity().getState().isFinished();
        }               
    }
}