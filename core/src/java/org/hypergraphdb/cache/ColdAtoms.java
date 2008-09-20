package org.hypergraphdb.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import org.hypergraphdb.util.SimplyLinkedQueue;

/**
 * The purpose of this class is just to keep references to atoms in the WeakRefAtomCache
 * for a little while so that they don't get evicted as soon as the garbage collector
 * collects them because they are not referred to anymore in the program. 
 * 
 * The term "cold" means "not really frozen" :)
 * 
 * @author Boris
 *
 */
public class ColdAtoms
{
	public static int DEFAULT_BUCKET_SIZE = 2000;
	int bucket_size;
	private SimplyLinkedQueue<Object []> buckets = new SimplyLinkedQueue<Object[]>();
	int pos = 0;
	
	private final ReferenceQueue<Object> queue = new ReferenceQueue<Object>();	
	SoftReference<Object> lowMemIndicator = new SoftReference<Object>(new Object(), queue);
	
	private void processQueue()
	{
		if (queue.poll() != null)
		{
			Runtime rt = Runtime.getRuntime();
			double free = rt.freeMemory();
			double max = rt.maxMemory();
			double used = (max - free)/max;
			if (used < 0.9) // percentage if used memory before we release it should be made configurable....
				return;
			if (buckets.size() > 1)
				buckets.fetch();
			// reset the low memory indicator
			lowMemIndicator = new SoftReference<Object>(new Object(), queue);
		}
	}
	
	public ColdAtoms()
	{
		this(DEFAULT_BUCKET_SIZE);
	}
	
	/**
	 * 
	 * @param capacity
	 * @param bucket_size
	 */
	public ColdAtoms(int bucket_size)
	{
		this.bucket_size = bucket_size; 
		buckets.put(new Object[bucket_size]);
	}
	
	// TODO: would it be beneficial to do more fine grained synchronization here?
	// not really clear...
	public synchronized void add(Object atom)
	{
		processQueue();
		if (pos >= bucket_size)
		{
			buckets.put(new Object[bucket_size]);
			pos = 0;
		}		
		buckets.peekBack()[pos++] = atom;
	}
}