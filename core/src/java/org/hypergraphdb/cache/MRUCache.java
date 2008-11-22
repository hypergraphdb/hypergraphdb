package org.hypergraphdb.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.util.ActionQueueThread;
import org.hypergraphdb.util.CloseMe;
import org.hypergraphdb.util.MemoryWarningSystem;
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
public class MRUCache<Key, Value> implements HGCache<Key, Value>, CloseMe
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
		Entry(Key key, Value value, Entry<Key, Value> next, Entry<Key, Value> prev)
		{
			this.key = key;
			this.value = value;
			this.next = next;
			this.prev = prev;
		}
	}
	
	private RefResolver<Key, Value> resolver;
	int maxSize = -1;
	float usedMemoryThreshold, evictPercent;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();	
	private Entry<Key, Value> top = null;
	private Entry<Key, Value> cutoffTail = null;
	private int cutoffSize = 0;
	private Map<Key, Entry<Key, Value>> map = new HashMap<Key, Entry<Key, Value>>();
	
	class ClearAction implements Runnable
	{
		public void run()
		{
			lock.writeLock().lock();
			map.clear();
			cutoffTail = top = null;
			cutoffSize = 0;
			lock.writeLock().unlock();			
		}
	}
	
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
				if (l.next != null)
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
	
	private void adjustCutoffTail()
	{
		if (cutoffTail == null)
		{
			cutoffTail = top;
			cutoffSize = map.size();
		}
		double desired = map.size()*evictPercent;
		while (cutoffSize > desired && cutoffTail.next != null)
		{
			cutoffTail = cutoffTail.next;
			cutoffSize--;
		}
		while (cutoffSize < desired && cutoffTail.prev != null)
		{
			cutoffTail = cutoffTail.prev;
			cutoffSize++;
		}		
	}
	
	class AddElement implements Runnable 
	{
		Entry<Key, Value> e;
		public AddElement(Entry<Key, Value> e)
		{
			this.e = e;
		}
		public void run()
		{
			lock.readLock().lock();
			try
			{
				if (!map.containsKey(e.key)) // it could have been removed before we got link it to the list
					return;
				e.next = top;
				if (top != null)			
					top.prev = e; 
				top = e;
			}
			finally
			{
				lock.readLock().unlock();
			}
			adjustCutoffTail();
		}
	}
	
	class EvictAction implements Runnable
	{
		public void run()
		{
			if (top == null)
				return;
/*			double used = (double)(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/
			(double)Runtime.getRuntime().maxMemory();
			System.out.println("EVICT ACTION CALLED -- " + map.size() + ", " + used);
			System.out.println("EVICT ACTION EVICTING :" + Runtime.getRuntime().freeMemory());
			int evicted = 0; */
			adjustCutoffTail();
			if (cutoffTail.prev != null)
				cutoffTail.prev.next = null;
			for (; cutoffTail != null; cutoffTail = cutoffTail.next)
			{
				lock.writeLock().lock();
				map.remove(cutoffTail.key);
				lock.writeLock().unlock();
//				evicted++;
			}
//			System.gc();
//			System.out.println("EVICTION COMPLETED :" + evicted + ", "  + Runtime.getRuntime().freeMemory());
		}
	}
	
	private MemoryWarningSystem.Listener memListener = new MemoryWarningSystem.Listener()
	{
		public void memoryUsageLow(long usedMemory, long maxMemory)
		{
			CacheActionQueueSingleton.get().pauseActions();
			try
			{
				new EvictAction().run();
			}
			finally
			{
				CacheActionQueueSingleton.get().resumeActions();
			}
		}
	};
	
	private void initMemoryListener()
	{
		HGEnvironment.getMemoryWarningSystem().addListener(memListener);
	}
	
	protected void finalize()
	{
		close();
	}
	
	public MRUCache()
	{
		initMemoryListener();
	}
	
	/** 
	 * @param maxSize The maximum number of elements allowed in the cache.
	 * @param evictCount The number of (least used) elements to evict when 
	 * the cache reaches its maximum.
	 */
	public MRUCache(int maxSize, int evictCount)
	{
		this();
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
		this();
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
			lock.writeLock().lock();
			e = map.get(key);
			if (e != null)
			{
				lock.writeLock().unlock();
				CacheActionQueueSingleton.get().addAction(new PutOnTop(e));
				return e.value;				
			}
			Value v = resolver.resolve(key);
			e = new Entry<Key, Value>(key, v, null, null);
			map.put(key, e);
			lock.writeLock().unlock();
			CacheActionQueueSingleton.get().addAction(new AddElement(e));
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
		ActionQueueThread aq = CacheActionQueueSingleton.get();
		aq.addAction(new ClearAction());
		aq.completeAll();
		
	}
	
	public void clearNonBlocking()
	{
		CacheActionQueueSingleton.get().addAction(new ClearAction());
	}
	
	/**
	 * Check that the map contains exactly the same elements as the linked list.
	 * Throw an exception if that is not the case. This method is intended for
	 * testing. Could be used in a production runtime for monitoring but it should
	 * be kept in mind that it might take quite a long time. The method will block all
	 * other activity on the cache. 
	 */
	public void checkConsistent()
	{
		
	}
	
	public void close()
	{
		HGEnvironment.getMemoryWarningSystem().removeListener(memListener);		
	}
}