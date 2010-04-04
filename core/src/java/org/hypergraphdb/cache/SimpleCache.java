/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.hypergraphdb.util.RefResolver;

public class SimpleCache<Key, Value> implements HGCache<Key, Value>
{	
	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private RefResolver<Key, Value> resolver;
	private Map<Key,Value> map = new HashMap<Key, Value>();
		
	public SimpleCache()
	{
	}
	
	public Value get(Key key)
	{
		lock.readLock().lock();
		Value v = map.get(key);
		lock.readLock().unlock();
		if (v == null)
		{
			v = resolver.resolve(key);
			lock.writeLock().lock();
			map.put(key, v);
			lock.writeLock().unlock();
			return v;
		}
		else
			return v;
	}

	public Value getIfLoaded(Key key)
	{
		lock.readLock().lock();
		Value v = map.get(key);
		lock.readLock().unlock();
		return v;
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
		map.remove(key);
		lock.writeLock().unlock();
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
		lock.writeLock().unlock();
	}

	public int size()
	{
	    return map.size();
	}	
}
