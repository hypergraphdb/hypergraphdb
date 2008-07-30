package org.hypergraphdb.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.util.RefResolver;

/**
 * 
 * <p>
 * Implements a cache that keeps most recently used elements in memory while
 * discarding the least recently used ones. Evicting elements is done in chunks
 * determined by a percentage of the current cache's size - see the constructors
 * for more info.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <Key>
 * @param <Value>
 */
public class MRUCache<Key, Value> implements HGCache<Key, Value>
{
	// The cache entry contains a lot of information here: 4 references = 16 bytes total.
	// But there's no escape since we need a doubly linked list if we are to freely move
	// an element from the middle to the top. Also, we clearly need the value there for
	// the get operation; finally, the key is needed during evict, to remove it from the
	// hash map itself. 
	//
	// Note: We could save 4 bytes per entry if we implemented the hash table
	// within this class instead of using the standard HashMap implementation, as the 
	// standard LinkedHashMap does. 
	static class Entry<Key, Value>
	{
		Key key;
		Value value;
		Entry<Key, Value> next;
		Entry<Key, Value> prev;
		Entry(Value value, Entry<Key, Value> next, Entry<Key, Value> prev)
		{
			this.value = value;
			this.next = next;
			this.prev = prev;
		}
	}
	
	private RefResolver<Key, Value> resolver;
	int maxSize = -1;
	double usedMemoryThreshold, evictPercent;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();	
	private Entry<Key, Value> top = null;
	private Entry<Key, Value> cutoffTail = null;
	private int cutoffSize = 0;
	private Map<Key, Entry<Key, Value>> map = new HashMap<Key, Entry<Key, Value>>();
	
	class PutOnTop implements Runnable
	{
		Entry<Key, Value> l;
		PutOnTop(Entry<Key, Value> l) 
		{
			this.l = l;
		}
		public void run()
		{
			lock.readLock().lock();
			try
			{
				 // If it's already on top or it's been removed, do nothing
				if (l.prev == null || !map.containsKey(l.key))
					return;
				if (l == cutoffTail)
					cutoffTail = l.prev;
				l.prev.next = l.next;
				l.next.prev = l.prev;
				l.next = top;
				l.prev = null;
				top.prev = l;
				top = l;
			}
			finally
			{
				lock.readLock().unlock();
			}
		}
	}
	
	class UnlinkEntry implements Runnable
	{
		Entry<Key, Value> e;
		UnlinkEntry(Entry<Key, Value> e) { this.e = e;}
		public void run()
		{
			if (e.prev != null)
				e.prev.next = e.next;
			if (e.next != null)
				e.next.prev = e.prev;
			e.prev = e.next = null;
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
			Entry<Key, Value> newEntry = null;
			lock.writeLock().lock();
			try
			{
				if (map.containsKey(key))
					return;
				newEntry = new Entry<Key, Value>(value, top, null); 
				map.put(key, newEntry);
			}
			finally { lock.writeLock().unlock(); }			
			
			if (top != null)			
				top.prev = newEntry; 
			top = newEntry;
			if (cutoffTail == null)
			{
				cutoffTail = top;
				cutoffSize = map.size();
			}
			else while (cutoffSize > map.size()*evictPercent && cutoffTail.next != null)
			{
				cutoffTail = cutoffTail.next;
				cutoffSize--;
			}
			if (maxSize >= map.size() || 
				usedMemoryThreshold >= 
				(double)(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory())/(double)Runtime.getRuntime().maxMemory());
			{
				if (cutoffTail.prev != null)
					cutoffTail.prev.next = null;
				for (; cutoffTail != null; cutoffTail = cutoffTail.next)
				{
					lock.writeLock().lock();
					map.remove(cutoffTail.key);
					lock.writeLock().unlock();
				}
			}
		}
	}
	
	public MRUCache()
	{		 
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
		this.evictPercent = (double)evictCount/(double)maxSize;
	}
	
	/** 
	 * @param usedMemoryThreshold The percentage of total memory that
	 * must become used before the cache decides to evict elements (e.g. 
	 * a value of 0.9 means the cache will evict elements when 90% of memory
	 * is currently in use). 
	 * @param evictPercent The percentage of elements to evict when the
	 * usedMemoryThreshold is reached.
	 */
	public MRUCache(double usedMemoryThreshold, double evictPercent)
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
		Entry<Key, Value> e = map.get(key);
		lock.readLock().unlock();
		if (e == null)
		{
			Value v = resolver.resolve(key);
			CacheActionQueueSingleton.get().addAction(new AddElement(key, v));
			return v;
		}
		else
		{
			CacheActionQueueSingleton.get().addAction(new PutOnTop(e));
			return e.value;
		}
	}

	public Value getIfLoaded(Key key)
	{
		lock.readLock().lock();
		Entry<Key, Value> e = map.get(key);
		lock.readLock().unlock();
		if (e == null)
			return null;
		else
		{
			CacheActionQueueSingleton.get().addAction(new PutOnTop(e));
			return e.value;
		}
	}
	
	public boolean isLoaded(Key key)
	{
		lock.readLock().lock();
		boolean b = map.containsKey(key);
		lock.readLock().unlock();
		return b;
	}
	
	public void remove(Key key)
	{
		lock.writeLock().lock();
		Entry<Key, Value> e = map.remove(key);
		lock.writeLock().unlock();
		if (e != null)
			CacheActionQueueSingleton.get().addAction(new UnlinkEntry(e));
	}
	
	public RefResolver<Key, Value> getResolver()
	{
		return resolver;
	}

	public void setResolver(RefResolver<Key, Value> resolver)
	{
		this.resolver = resolver;
	}
	
	public void clear()
	{
		lock.writeLock().lock();
		map.clear();
		cutoffTail = top = null;
		cutoffSize = 0;
		lock.writeLock().unlock();
	}
}