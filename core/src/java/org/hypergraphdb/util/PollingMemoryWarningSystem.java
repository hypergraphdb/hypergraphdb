/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import org.hypergraphdb.HGEnvironment;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
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
 */
public class PollingMemoryWarningSystem implements MemoryWarningSystem
{
	private final List<Listener> listeners = new CopyOnWriteArrayList<>();
	private MemoryPoolMXBean tenuredGenPool = null;
	private long threshold;
	

	public PollingMemoryWarningSystem()
	{
	    tenuredGenPool = this.findTenuredGenPool();
		Thread t = new Thread(() -> {
			while (true)
			{
				MemoryUsage usage = tenuredGenPool.getUsage();
				if (usage.getUsed() > threshold)
				{
					
					long maxMemory = tenuredGenPool.getUsage().getMax();
					long usedMemory = tenuredGenPool.getUsage().getUsed();
					for (Listener listener : listeners)
					{
						try
						{
							listener.memoryUsageLow(usedMemory, maxMemory);
						}
						catch (Throwable err)
						{
							System.err.println("Error in memory threshold exceeded listener");
							err.printStackTrace();
						}
					}
					
					
					try
					{
						Thread.sleep(1000);
					}
					catch (InterruptedException e)
					{
						throw new RuntimeException(e);
					}
				}
			}
			
		}, "MemoryUsageMonitor");
		t.setDaemon(true);
		t.start();
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
		this.threshold = (long) (maxMemory * percentage);
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
