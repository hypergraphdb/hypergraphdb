/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import org.hypergraphdb.util.ActionQueueThread;

/**
 * 
 * <p>
 * Wrap a single instance of {@link ActionQueueThread} for use by all caches in a 
 * JVM instance. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class CacheActionQueueSingleton
{
	private static ActionQueueThread queue = new ActionQueueThread("HyperGraph Cache Maintenance");
	
	static
	{
		queue.setPriority(Thread.NORM_PRIORITY + 3);
		queue.setDaemon(true);
		queue.start();
	}
	
	public static ActionQueueThread get() { return queue; } 
}
