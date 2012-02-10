/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.util.MemoryWarningSystem;
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
	int evictFactor = 3; // evict 1/3 of elements when memory is about to fill up
	private SimplyLinkedQueue<Object []> buckets = new SimplyLinkedQueue<Object[]>();
	int pos = 0;
	
	private MemoryWarningSystem.Listener memListener = new MemoryWarningSystem.Listener()
	{
		public void memoryUsageLow(long usedMemory, long maxMemory)
		{
//			System.out.println("FREE COLD ATOMS START " + Runtime.getRuntime().freeMemory() + " - " + buckets.size());
			synchronized (buckets)
			{
				int cnt = buckets.size() / evictFactor;
				if (cnt == 0)
				    cnt = buckets.size() > 0 ? 1 : 0;
				while (cnt-- > 0)
					buckets.fetch();
			}				
			//2012.02.02 System.gc();
//			System.out.println("FREE COLD ATOMS END " + Runtime.getRuntime().freeMemory() + " - " + buckets.size());
		}
	};
	
	private void initMemoryListener()
	{
		HGEnvironment.getMemoryWarningSystem().addListener(memListener);
	}
	
	public ColdAtoms()
	{
		this(DEFAULT_BUCKET_SIZE);
		initMemoryListener();
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
	
	public void add(Object atom)
	{
		synchronized (buckets)
		{
			if (pos >= bucket_size || buckets.size() == 0)
			{
				buckets.put(new Object[bucket_size]);
				pos = 0;
			}		
			buckets.peekBack()[pos++] = atom;
		}
	}
	
	public int size()
	{
	    return bucket_size*buckets.size();
	}
}
