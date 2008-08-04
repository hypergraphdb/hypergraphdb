package org.hypergraphdb.cache;

import org.hypergraphdb.util.ActionQueueThread;

/**
 * 
 * <p>
 * Wrap a single instance of {@link ActionQueueThread} for use by all caches in a 
 * HyperGraphDB instance. 
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
		queue.start();
	}
	
	public static ActionQueueThread get() { return queue; } 
}