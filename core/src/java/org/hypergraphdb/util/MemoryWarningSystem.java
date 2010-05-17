/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import javax.management.*;

import org.hypergraphdb.HGEnvironment;

import java.lang.management.*;
import java.util.*;

/**
 * <p>
 * This memory warning system will call all registered listeners when we
 * exceed the percentage of available heap memory specified.  
 * </p>
 * 
 * <p>
 * There should only be one instance of this object created, since the
 * usage threshold can only be set to one number. A HyperGraphDB JVM-wide
 * singleton is available statically from {@link HGEnvironment} class. 
 * </p>
 * 
 * <p>HyperGraph will configure a default usage threshold percentage for
 * the HEAP (i.e. "tenure generation") JVM memory pool to 0.9. This means that
 * listeners will be invoked when used memory reaches about 90% of the maximum
 * available memory. You can change that percentage by calling the
 * <code>setPercentageUsage</code> method and that will globally affect the behavior
 * of all running code. This is an unfortunate design of the JVM - it doesn't
 * allow more than threshold to be configured. It's possible for one to write
 * one's own thread that monitor heap usage, but this begs the question whether
 * the overhead is worth it.
 * </p>
 * 
 * <p>
 * <b>NOTE:</b> The goal of this is clearly to reduce memory consumption in caches and the like.
 * Because each listener will shrink memory in an independent way, the result of
 * invoking all of them will perhaps lead to an overzealous cleanup of useful
 * cached information. In the future, it would be a good idea to architect some 
 * sort of cooperation of such cleanup code. Perhaps a desired threshold could be set
 * and each listener invoked iteratively to perform "a little bit of cleanup" as many
 * times as needed to reach that threshold.  
 * </p>
 * 
 * <p>
 * <em>Code taken from http://www.roseindia.net/javatutorials/OutOfMemoryError_Warning_System.shtml
 * </em></p>
 */
public class MemoryWarningSystem 
{
	private final Collection<Listener> listeners = new ArrayList<Listener>();
	private MemoryPoolMXBean tenuredGenPool = null;
	
	public static interface Listener 
	{
		void memoryUsageLow(long usedMemory, long maxMemory);
	}

	public MemoryWarningSystem() 
	{
	    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
	    tenuredGenPool = this.findTenuredGenPool();
	    NotificationEmitter emitter = (NotificationEmitter) mbean;
	    emitter.addNotificationListener(new NotificationListener() 
	    {
	    	public void handleNotification(Notification n, Object hb) 
	    	{
	    		if (n.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) 
	    		{	    			
		            long maxMemory = tenuredGenPool.getUsage().getMax();
		            long usedMemory = tenuredGenPool.getUsage().getUsed();
		            for (Listener listener : listeners) 
		            {
		              listener.memoryUsageLow(usedMemory, maxMemory);
		            }
	    		}
	    	}
	    }, null, null);
	}

	public boolean addListener(Listener listener) 
	{
		return listeners.add(listener);
	}

	public boolean removeListener(Listener listener) 
	{
		return listeners.remove(listener);
	}

	public void setPercentageUsageThreshold(double percentage) 
	{
		if (percentage <= 0.0 || percentage > 1.0) 
			throw new IllegalArgumentException("Percentage not in range");

	    long maxMemory = tenuredGenPool.getUsage().getMax();
	    long warningThreshold = (long) (maxMemory * percentage);
	    tenuredGenPool.setUsageThreshold(warningThreshold);	    
	}

	public double getPercentageUsageThreshold()
	{
	    long maxMemory = tenuredGenPool.getUsage().getMax();
	    long warningThreshold = tenuredGenPool.getUsageThreshold(); 
	    return (double)warningThreshold/(double)maxMemory;
	}
	
	/**
     * Tenured Space Pool can be determined by it being of type
     * HEAP and by it being possible to set the usage threshold.
     */
	private MemoryPoolMXBean findTenuredGenPool() 
	{
	    MemoryPoolMXBean last = null;
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) 
		{
			// I don't know whether this approach is better, or whether
			// we should rather check for the pool name "Tenured Gen"?
			if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported())
			{
				last = pool;
//				System.out.println("pool " + pool);
			}
		}
		if (last != null)
		    return last;
		else
		    throw new AssertionError("Could not find tenured space");
	}
}
