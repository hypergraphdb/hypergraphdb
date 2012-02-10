/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.LinkedList;
import java.util.concurrent.Semaphore;

/**
 * <p>
 * This a simple queue that runs as a thread and executes passed
 * in action object (i.e. <code>Runnable</code> instances) in a 
 * sequence. It can be configured with a maximum size. When this maximum
 * size is reached and a new element is added to it, it will not return
 * until some percentage of the accumulated actions has been completed.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ActionQueueThread extends Thread 
{
	public static final int DEFAULT_NON_BLOCKING_SIZE = 100000;
	public static final int DEFAULT_FREE_PERCENT_ON_BLOCK = 50;
	
	/**
	 * Thread can be paused every n actions.
	 */
	public static final int PAUSE_GRANULARITY_ACTIONS = 100;
	
	
	private HGLogger logger = new HGLogger();
	
	/**
	 * The current list of actions to execute.
	 */
	private LinkedList<Runnable> actionList = new LinkedList<Runnable>();
	
	/**
	 * The max size of the action queue before it block a calling thread.
	 */
	private int nonBlockingSize = DEFAULT_NON_BLOCKING_SIZE;
	
	/**
	 * The percentage of actions of a filled up queue that must be
	 * completed before we unblock a calling thread.
	 */
	// 2012.02.01 hilpold Bugfix private double freeFactor = 1.0 - 1.0/DEFAULT_FREE_PERCENT_ON_BLOCK;
	// 
	private double freeFactor = 1.0 - DEFAULT_FREE_PERCENT_ON_BLOCK / 100.0;
	
	/**
	 * A flag indicating whether the thread is currently running. Set by clients
	 * to cause the main loop to exit.
	 */
	private boolean running = false;
	
	/**
	 * The total number of actions executed by this thread, whether or not
	 * the actions have terminated with an exception.
	 */
	private long completedCount = 0;
	
	/**
	 * The thread can be paused and resumed at the granularity of PAUSE_GRANULARITY_ACTIONS
	 * actions.
	 */
	private Semaphore pauseMutex = new Semaphore(1);	
	
	/**
	 * <p>Default constructor. Unnamed action queue with a default
	 * max size.
	 * </p> 
	 */
	public ActionQueueThread()
	{
	}

	/**
	 * <p>Constructs an <code>ActionQueue</code> with a specific thread
	 * name and a default max size.
	 * </p> 
	 * 
	 * @param name The name of the action queue thread.
	 */
	public ActionQueueThread(String name)
	{
		super(name);
	}

	/**
	 * <p>Constructs an <code>ActionQueue</code> with a specific thread
	 * name and max size.
	 * </p> 
	 * 
	 * @param name The name of the action queue thread.
	 * @param maxSizeBeforeBlock The maximum number of actions waiting in the queue
	 * before this <code>ActionQueue</code> blocks a calling thread.
	 * @param completPercentUponBlocking The amount, in percent, of action to execute
	 * from a filled up queue before we return to the blocked thread. 
	 */
	public ActionQueueThread(String name, 
							 int maxSizeBeforeBlock, 
							 int completePercentUponBlocking)
	{
		this(name);
		this.nonBlockingSize = maxSizeBeforeBlock;
		// 2012.02.01 hilpold BugFix freefactor calculation wrong, values were too high.
		// Observed low processor load on long tasks. 
		// this.freeFactor = 1.0 - 1.0/completePercentUponBlocking;
		this.freeFactor = 1.0 - completePercentUponBlocking / 100.0;
	}
	
	public void run()
	{
		int pauseActionCounter = 0;
		for (running = true; running; )
		{			
			Runnable action = null;
			synchronized (actionList)
			{
				while (actionList.isEmpty() && running)
					try { actionList.wait(1000); }
					catch (InterruptedException ex) { running = false; }
			    if (actionList.isEmpty())
			    	break;
				action = (Runnable)actionList.removeFirst();
			}
			
			try
			{	
				if (pauseActionCounter == 0) {
					// acquiring mutex is slow
					pauseMutex.acquire();
				}
				action.run();
				pauseActionCounter ++;
			}
			catch (InterruptedException ex)
			{
				break;
			}
			catch (Throwable t)
			{
				logger.exception(t);
			}
			finally
			{
				completedCount++;
				if (pauseActionCounter == PAUSE_GRANULARITY_ACTIONS) {
					pauseMutex.release();
					pauseActionCounter = 0;
				}
			}
		}
		
		//
		// Complete pending actions after thread stopped.
		//
		while (!actionList.isEmpty())
		{
			Runnable action = (Runnable)actionList.removeFirst();
			action.run();
		}
	}
		
	/**
	 * <p>Suspend the execution of actions until the <code>resumeActions</code> method is
	 * called. Blocks until all current actions (@see PAUSE_GRANULARITY_ACTIONS)
	 * complete execution.</p>
	 */
	public void pauseActions()
	{		
		try
		{
			pauseMutex.acquire();
		}
		catch (InterruptedException ex) { }
	}
	
	/**
	 * <p>Resume action processing previously paused by a call to <code>pauseActions</code>.</p>
	 */
	public void resumeActions() 
	{ 
		pauseMutex.release(); 
	}

	public void addAction(Runnable action)
	{
		//
		// Make sure we don't store too many elements in the update list.
		// Block while update list is larger than the allowed maximum
		// non-blocking size.
		//
		if (actionList.size() > nonBlockingSize)
		{
//			this.setPriority(Thread.NORM_PRIORITY + 2);
			
			while (actionList.size() > nonBlockingSize * freeFactor)
			{
				try
				{
					//2012.02.06 hilpold decreaesed sleep after profiling Thread.sleep(50);
					//2012.02.06 11:30 Thread.sleep(SLEEP_TIME);
					//2012.02.06 12:00 Thread.yield();
					Thread.sleep(0, 100);
				}
				catch (InterruptedException ex)
				{
					break;
				}
			}
		}
		synchronized (actionList)
		{
			actionList.addLast(action);
			//2012.02.06 actionList.notify();
			actionList.notifyAll();
		}
	}
	
	/**
	 * <p>Put an action in front of the queue so that it's executed 
	 * next or close to next. This method will not block even
	 * if the size of the accumulated actions exceeds the blocking
	 * threshold </p>
	 */
	public void prependAction(Runnable action)
	{
		synchronized (actionList)
		{
			actionList.addFirst(action);
			actionList.notify();
		}
	}
	
	/**
	 * <p>Complete all scheduled actions at the time of this call. Since other
	 * threads may keep adding actions, this method makes sure that only
	 * the actions in the queue at the time of the call are waited upon. 
	 * </p>
	 *
	 */
	public void completeAll()
	{
		long currentCompleted = completedCount;		
		// ActionList.size() may be 0, but we may still an action currently running so we
		// wait for at least one action to complete.
		int size = Math.max(1, actionList.size()); 
		while (completedCount - currentCompleted < size)
		{
			// if the thread is waiting or it's terminated, we have no business staying in here...
			if (getState() != Thread.State.BLOCKED && getState() != Thread.State.RUNNABLE)
				break;			
			try { Thread.sleep(50); } 
			catch (InterruptedException ex) { break; }
		}
	}

	/**
	 * <p>Clear all actions. The currently executing actions will still complete,
	 *  but all others will be removed.</p>
	 */
	public void clearAll()
	{
		synchronized (actionList)
		{
			actionList.clear();
		}
	}
	
	/**
	 * <p>Return the total number of actions executed by this thread, whether or not
	 * the actions have terminated with an exception.</p>
	 */	
	public long getCompletedCount()
	{
		return this.completedCount;
	}
	public boolean isRunning()
	{
		return running;
	}
	
	public void stopRunning()
	{
		running = false;
		synchronized (actionList)
		{
			actionList.notify();
		}
	}
}
