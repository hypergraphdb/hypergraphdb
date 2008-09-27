package org.hypergraphdb.peer.workflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.util.Pair;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.Structs.*;


/**
 * @author Cipri Costa
 *
 * @param <StateType>
 * 
 * Base class for tasks. A task is an <code>AbstractActivity</code> that manages certain conversations and 
 * uses the state changes in the conversations as triggers for transactions in own state. Unlike Conversation 
 * activities, messages are accepted even if the task is not in a state where a transition is possible with that message.
 * This class ensures implementors that those messages are saved and will trigger transformations when (if ever) the task 
 * enters an appropriate state.
 * 
 * The task is also registered against the <code>PeerInterface</code> and routes all messages received through that 
 * interface to the appropriate conversations.
 * 
 * 
 */
public abstract class TaskActivity<StateType> extends AbstractActivity<StateType> implements ActivityStateListener
{
	private UUID taskId;
	private PeerInterface peerInterface;
	
	/**
	 * map of active conversations
	 */
	private HashMap<UUID, Conversation<?>> conversations = new HashMap<UUID, Conversation<?>>();
	/**
	 * each state has a queue of messages. When the task reaches the state, the activity triggers a transaction.
	 */
	private HashMap<StateType, LinkedBlockingQueue<AbstractActivity<?>>> activityQueues = new HashMap<StateType, LinkedBlockingQueue<AbstractActivity<?>>>();
	/**
	 * Used to detrmine the queue of an activity based on the change in the activity state
	 */
	private HashMap<Object, StateType> conversationQueueMaping = new HashMap<Object, StateType>();
	/**
	 * map of registered transitions. Each transition also triggers a function that is implemented in the child classes
	 */
	private HashMap<Pair<StateType, Object>, Pair<StateType, Method>> transitions = new HashMap<Pair<StateType,Object>, Pair<StateType,Method>>();
	
	/**
	 * Conversation states we are interested in
	 */
	private HashSet<Object> conversationStates = new HashSet<Object>(); 

	public TaskActivity(PeerInterface peerInterface, StateType start, StateType end)
	{
		this(peerInterface, UUID.randomUUID(), start, end);
	}
	
	public TaskActivity(PeerInterface peerInterface, UUID taskId, StateType start, StateType end)
	{
		super(start, end);
		
		this.peerInterface = peerInterface;
		this.taskId = taskId;
				
		peerInterface.registerTask(taskId, this);
	}

	/**
	 * Executes a transition
	 * 
	 * @param method
	 * @param activity
	 */
	protected void handleActivity(Method method, AbstractActivity<?> activity)
	{
		try
		{
			StateType targetState = (StateType) method.invoke(this, activity);
			setState(targetState);
		
		} catch (IllegalArgumentException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/* (non-Javadoc)
	 * @see org.hypergraphdb.peer.workflow.ActivityStateListener#stateChanged(java.lang.Object, org.hypergraphdb.peer.workflow.AbstractActivity)
	 * 
	 * Called by conversations when their state changes.
	 */
	public void stateChanged(Object newState, AbstractActivity<?> activity)
	{
		System.out.println("TaskActivity: conversation state changed to " + newState + " while in " + getState() );
		
		StateType interestedState = conversationQueueMaping.get(newState);
		
		LinkedBlockingQueue<AbstractActivity<?>> queue = activityQueues.get(interestedState);

		if (queue != null)
		{
			try
			{
				System.out.println("queueing message for " + interestedState);
				queue.put(activity);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			System.out.println("can not find queue for " + interestedState);
		}
	}

	/**
	 * @param msg
	 * 
	 * Called by the peer interface when a message arrives for this task.
	 */
	public void handleMessage(Object msg)
	{
		System.out.println("TaskActivity: handleMessage ");
		Conversation<?> conversation = conversations.get(getPart(msg, CONVERSATION_ID));//.getConversationId());
		if (conversation == null)
		{
			conversation = createNewConversation(msg);
			
			if (conversation != null) registerConversation(conversation, (UUID)getPart(msg, CONVERSATION_ID));
		}
		
		if (conversation != null) conversation.handleIncomingMessage(msg);
	}

	protected abstract void startTask();
	
	protected void doRun()
	{
		startTask();
		
		while (!isStopped())
		{
			//get queue for current state
			LinkedBlockingQueue<AbstractActivity<?>> queue = activityQueues.get(getState());
	
			if (queue != null)
			{
				//wait for input on that
				AbstractActivity<?> activity = null;
				try
				{
					System.out.println("wating on queue for " + getState());
					activity = queue.take();
				} catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (activity != null)
				{
					//get step info
					Pair<StateType, Method> dest = transitions.get(new Pair<StateType, Object>(getState(), activity.getState()));
					
					if (compareAndSetState(getState(), dest.getFirst()))
					{
						handleActivity(dest.getSecond(), activity);
					}
				}
			}else{
				//wait for state changes
				System.out.println("No queue found for " + getState() + " in " + this.getClass());

				try
				{
					stateChangedLatch.await();
				} catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	protected void registerConversation(Conversation<?> conversation, UUID conversationId)
	{

		if (conversation != null)
		{
			//add handlers
			for(Object conversationState : conversationStates)
			{
				conversation.setStateListener(conversationState, this);
			}
			conversations.put(conversationId, conversation);
		}
		
	}

	protected Conversation<?> createNewConversation(Object msg)
	{
		return null;
	}

	protected void registerConversationHandler(StateType fromState, Object conversationState, String functionName, StateType toState)
	{
		//create a new input queue
		if (!activityQueues.containsKey(fromState))
		{
			LinkedBlockingQueue<AbstractActivity<?>> queue = new LinkedBlockingQueue<AbstractActivity<?>>();
			activityQueues.put(fromState, queue);
		}
		
		//add transition
		try
		{
			Method method = this.getClass().getMethod(functionName, AbstractActivity.class);
			
			transitions.put(new Pair<StateType, Object>(fromState, conversationState), new Pair<StateType, Method>(toState, method));
		} catch (SecurityException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		conversationQueueMaping.put(conversationState, fromState);
		//remember conversation state
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
	
	public PeerInterface getPeerInterface()
	{
		return peerInterface;
	}
	public void setPeerInterface(PeerInterface peerInterface)
	{
		this.peerInterface = peerInterface;
	}
}
