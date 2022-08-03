/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.hypergraphdb.HGEnvironment;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
public class OnGCCheckMemoryWarningSystem implements MemoryWarningSystem
{
	private final Collection<Listener> listeners = new ArrayList<Listener>();
	private MemoryPoolMXBean tenuredGenPool = null;
	private long threshold;
	
	
	public OnGCCheckMemoryWarningSystem()
	{
		List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
		tenuredGenPool = findTenuredGenPool();
		
		for (GarbageCollectorMXBean gcbean : gcbeans)
		{
			NotificationEmitter emitter = (NotificationEmitter) gcbean;
			emitter.addNotificationListener(new NotificationListener()
			{
				public void handleNotification(Notification n, Object hb)
				{
					String notificationType = n.getType();
					if (notificationType.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
					{
						CompositeData cd = (CompositeData) n.getUserData();
						GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
						MemoryUsage usage = info.getGcInfo().getMemoryUsageAfterGc().get(tenuredGenPool.getName());
						if (usage == null)
							throw new RuntimeException(String.format("Could not find memory usage report after GC for pool %s", tenuredGenPool.getName()));
						
						if(usage.getUsed() > threshold)
						{
							for (Listener listener : listeners)
							{
								System.out.println("Notifying listener " + listener);
								try
								{
									listener.memoryUsageLow(usage.getUsed(), usage.getMax());
								}
								catch (Throwable err)
								{
									System.err.println("Error in memory threshold exceeded listener");
									err.printStackTrace();
								}
							}
						}
					}
				}
			}, null, null);
			
		}
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
		threshold = (long) (maxMemory * percentage);
	}
	
	public double getPercentageUsageThreshold()
	{
		long maxMemory = tenuredGenPool.getUsage().getMax();
		return (double)threshold/(double)maxMemory;
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
