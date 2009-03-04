package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.workflow.WorkflowState.*;
import static org.hypergraphdb.peer.Structs.*;

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

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.protocol.Performative;


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
                long diff = (st-right.lastActionTimestamp)*right.queue.size()-
                            (st-left.lastActionTimestamp)*left.queue.size(); 
                return diff > 0 ? 1 : diff < 0 ? -1 : 0;
            }
        }
    );

    private Thread schedulerThread = new Thread("HGDB Peer Scheduler") 
    {
        public void run()
        {
            while (true)
            {
                try
                {
                    Activity a = globalQueue.take();
//                    System.out.println("activity pulled:" + a + "," + a.lastActionTimestamp + "," + a.future.waiting);
                    if (!a.queue.isEmpty())
                    {
                        Runnable r = a.queue.take();
                        System.out.println("Found action " + r + " in queue " + a); 
                        thisPeer.getExecutorService().execute(r);
                    }
                    else 
                    {
                        if (globalQueue.isEmpty())                    
                            Thread.sleep(100); // really? sleep here? that much?
                        a.lastActionTimestamp = System.currentTimeMillis();
                        globalQueue.put(a);
                    }
//                    Thread.sleep(1000);
//                    System.out.println("global queue size=" + globalQueue.size());
                }
                catch (InterruptedException ex) { break; }
            }
        }
    };
    
    private void handleActivityException(Activity activity, Throwable exception, Message msg)
    {
        activity.future.result.exception = exception;
        activity.getState().assign(WorkflowState.Failed); // TODO: what if already in ending state?
        exception.printStackTrace(System.err);
    }
    
    private Activity findRootActivity(Activity a)
    {
        Activity root = a; 
        for (Activity tmp = parents.get(root); tmp != null; root = tmp);
        return root;
    }
    
    private void notUnderstood(final Message msg, final String exlanation)
    {
        try 
        { 
            Object reply = combine(Messages.getReply(msg), 
                                   struct(PERFORMATIVE, Performative.NotUnderstood));
            // TODO: pass in 'explanation' somehow here...          
            Object replyTarget = getPart(msg, REPLY_TO);
            if (replyTarget != null)
                thisPeer.getPeerInterface().send(replyTarget, reply);
            else
                throw new Exception("Unknown reply target for message : " + msg);            
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
                try 
                {
                    Transition transition = 
                        type.getTransitionMap().getTransition(parentActivity.getState().getConst(), 
                                                              activity, 
                                                              activity.getState().getConst());
                    
                    if (transition == null)
                        return;
                    WorkflowStateConstant result = transition.apply(parentActivity, activity);
                    parentActivity.getState().assign(result);
                }
                catch (Throwable t)
                {
                    handleActivityException(parentActivity, t, null);
                }
                finally
                {
                    parentActivity.lastActionTimestamp = System.currentTimeMillis();
                    Activity rootActivity = findRootActivity(parentActivity);
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
                                          final Activity activity, 
                                          final Message msg)
    {
        return new Runnable() {
            public void run()
            {
                try 
                {
                    Transition transition = 
                        type.getTransitionMap().getTransition(activity.getState().getConst(), 
                                                              msg);
                    if (transition == null)
                    {
                        System.out.println("Can't make transition for " + activity + " and msg=" + msg);
                        notUnderstood(msg, " no state transition defined for this performative.");
                    }
                    else
                        System.out.println("Running transition " + transition + " on msg " + msg);
                    WorkflowStateConstant result = transition.apply(activity, msg);
                    System.out.println("Transition finished with " + result);
                    activity.getState().assign(result);
                }
                catch (Throwable t)
                {
                    handleActivityException(activity, t, msg);
                }
                finally
                {
                    activity.lastActionTimestamp = System.currentTimeMillis();                    
                    Activity rootActivity = findRootActivity(activity);
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
    
    private Runnable makeMessageHandleAction(final Activity activity, 
                                             final Message msg)
    {
        return new Runnable() {
            public void run()
            {
                try 
                {
                    activity.handleMessage(msg);
                }
                catch (Throwable t)
                {
                    handleActivityException(activity, t, msg);
                }
                finally
                {
                    activity.lastActionTimestamp = System.currentTimeMillis();                    
                    Activity rootActivity = findRootActivity(activity);
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
        schedulerThread.start();
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
        synchronized (activities)
        {
            if (activities.containsKey(activity.getId()))
                throw new RuntimeException("Activity " + activity + 
                                           " with ID " + activity.getId() + " has already been initiated.");

            activities.put(activity.getId(), activity);
        }
        ActivityFuture future = insertNewActivity(activity, parentActivity, listener);
        activity.getState().compareAndAssign(Limbo, Started);        
        activity.initiate();                       
        return future;
    }
    
    private ActivityFuture insertNewActivity(final Activity activity, 
                                             final Activity parentActivity,
                                             final ActivityListener listener)
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final ActivityFuture future = new ActivityFuture(activity, completionLatch);
        activity.future = future;        
        activity.getState().addListener(new StateListener() {
            public void stateChanged(WorkflowState state)
            {
                System.out.println("Activity state change " + activity + " to " + state.toString());
                if (state.isFinished())
                {
                    System.out.println("Activity " + activity + " is finished.");
                    completionLatch.countDown();
                    if (listener != null)
                        try { listener.activityFinished(future.get()); }
                        catch (Throwable t) { t.printStackTrace(System.err); /* this shouldn't ever happen! */}
                    // TODO: is it really correct to remove the activity from the map at this point?
                    // what if we get a message about this activity afterwards? Maybe activities should 
                    // be designed in such a way that the message content should be such that either
                    // it's ok to create a brand new activity from it, or it should be refused with a
                    // "not-understood" performative.
                    globalQueue.remove(activity);
                    activities.remove(activity.getId());
                }
            }
        });
        if (parentActivity != null)
        {
            // Serialize actions of all children within parent action queue so that states
            // changes are serialized and handled in order.
            activity.queue = parentActivity.queue; 
            activity.getState().addListener(new StateListener() { 
                public void stateChanged(WorkflowState state)
                {
                    ActivityType pt = activityTypes.get(parentActivity.getType());
                    parentActivity.queue.add(makeTransitionAction(pt, parentActivity, activity));
                }            
            });
        }
        else
        try
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
    
    public void handleMessage(final Message msg)
    {        
        System.out.println("Received message " + msg);
        UUID activityId = getPart(msg,  CONVERSATION_ID);
        if (activityId == null)
        {
            notUnderstood(msg, " missing conversation-id in message");
            return;            
        }
        
        // TODO: how is this synchronized in case several peers send something
        // about this same activity?
        Activity activity = activities.get(activityId);
        ActivityType type = null;
        if (activity == null)
        {
            Activity parentActivity = null;            
            UUID parentId = getPart(msg, PARENT_SCOPE);
            if (parentId != null)
            {
                parentActivity = activities.get(parentId);
                if (parentActivity == null)
                {
                    // Or should we just ignore the fact that we don't know about
                    // the parent activity here? It seems ok that sub-activities
                    // could be b/w a different group of peers than the parent
                    // activities
                    notUnderstood(msg, " unkown parent activity " + parentId);
                    return;
                }
            }
            type = activityTypes.get(getPart(msg, ACTIVITY_TYPE));
            if (type == null)
            {
                notUnderstood(msg, " unkown activity type '" + type + "'");
                return;                
            } 
            activity = type.getFactory().make(thisPeer, activityId, msg);
            insertNewActivity(activity, parentActivity, null);
            System.out.println("inserted new activity in queue ");
            activity.getState().compareAndAssign(Limbo, Started);            
        }
        else
            type = activityTypes.get(activity.getType());
        try
        {
            if (activity instanceof FSMActivity)
                activity.queue.put(makeTransitionAction(type, activity, msg));
            else
                activity.queue.put(makeMessageHandleAction(activity, msg));
            System.out.println("Added transition action to " + activity + " on msg  "+ msg);
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