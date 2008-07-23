package org.hypergraphdb.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.util.RefResolver;

/**
 * 
 * <p>
 * Implements a cache that keeps most recently used elements in memory while
 * discarding the least recently used ones.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <Key>
 * @param <Value>
 */
public class MRUCache<Key, Value> implements HGCache<Key, Value>
{
	private RefResolver<Key, Value> resolver;
	int maxSize = -1;
	float usedMemoryThreshold, evictPercent;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Map<Key, Value> map = new HashMap<Key, Value>();
	private Linked top = null;
	private Linked cutoffTail = null;
	private int cutoffSize = 0;
	
	class Linked
	{
		Key key;
		Linked next;
		Linked(Key key, Linked next)
		{
			this.key = key;
			this.next = next;
		}
	}
	
	class AddElement implements Runnable 
	{
		Key key;
		Value value;
		public AddElement(Key key, Value value)
		{
			this.key = key;
			this.value = value;			
		}
		public void run()
		{
			lock.writeLock().lock();
			try
			{
				map.put(key, value);				
				top = new Linked(key, top);
				if (cutoffTail == null)
				{
					cutoffTail = top;
					cutoffSize = map.size();
				}
				else while (cutoffSize > map.size()*evictPercent)
				{
					cutoffTail = cutoffTail.next;
					cutoffSize--;
				}
				if (maxSize >= map.size() || 
					usedMemoryThreshold >= 
					(float)(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory())/(float)Runtime.getRuntime().maxMemory());
				{
					for (; cutoffTail != null; cutoffTail = cutoffTail.next)
						map.remove(cutoffTail.key);
					cutoffTail = top;
					cutoffSize = map.size();
				}
			}
			finally
			{
				lock.writeLock().unlock();
			}
		}
	}
	
	/** 
	 * @param maxSize The maximum number of elements allowed in the cache.
	 * @param evictCount The number of (least used) elements to evict when 
	 * the cache reaches its maximum.
	 */
	public MRUCache(int maxSize, int evictCount)
	{
		this.maxSize = maxSize;
		if (maxSize <= 0) 
			throw new IllegalArgumentException("maxSize <= 0");
		else if (evictCount <= 0)
			throw new IllegalArgumentException("evictCount <= 0");			
		this.evictPercent = (float)evictCount/(float)maxSize;
	}
	
	/** 
	 * @param usedMemoryThreshold The percentage of total memory that
	 * must become used before the cache decides to evict elements (e.g. 
	 * a value of 0.9 means the cache will evict elements when 90% of memory
	 * is currently in use). 
	 * @param evictPercent The percentage of elements to evict when the
	 * usedMemoryThreshold is reached.
	 */
	public MRUCache(float usedMemoryThreshold, float evictPercent)
	{
		if (usedMemoryThreshold <= 0)
			throw new IllegalArgumentException("usedMemoryThreshold <= 0");		
		this.usedMemoryThreshold = usedMemoryThreshold;
		if (evictPercent <= 0)
			throw new IllegalArgumentException("evictPercent <= 0");		
		this.evictPercent = evictPercent;
	}
	
	public Value get(Key key)
	{
		lock.readLock().lock();
		Value v = map.get(key);
		lock.readLock().unlock();
		if (v == null)
		{
			v = resolver.resolve(key);
			CacheActionQueueSingleton.get().addAction(new AddElement(key, v));
		}
		return v;
	}

	public RefResolver<Key, Value> getResolver()
	{
		return resolver;
	}

	public void setResolver(RefResolver<Key, Value> resolver)
	{
		this.resolver = resolver;
	}
}