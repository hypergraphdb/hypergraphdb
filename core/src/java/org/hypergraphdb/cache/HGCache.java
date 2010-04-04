/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import org.hypergraphdb.util.RefResolver;

/**
 * 
 * <p>
 * A simple generic, read-only caching interface. An implementation must be initialized
 * by providing the means to load data that is not in the cache - a
 * <code>RefResolver</code> instance. Therefore, there's no <code>put</code>
 * method, the cache makes the decision if and when to actually keep data in it. Consequently,
 * the cache is operational only when there is <code>RefResolver</code> currently in effect.
 * Otherwise, the <code>get</code> will simply throw a <code>NullPointerException</code>
 * when attempting to access it.
 * </p>
 * 
 * <p>
 * Note, however, that there is a <code>remove</code> to explicitly remove an element from
 * the cache. This method should generally be called only when the item is being removed
 * from permanent storage as well.
 * </p>
 * 
 * <p>
 * When and how element are purged from the cache is not mandated by the interface. 
 * </p>
 * 
 * <p>
 * <strong>NOTE</strong>: Implementation are expected to be thread-safe.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGCache<Key, Value>
{
	/**
	 * <p>Set the <code>RefResolver</code> to be used to load data in the cache.</p>
	 */
	void setResolver(RefResolver<Key, Value> resolver);
	
	/**
	 * <p>Return the <code>RefResolver</code> used to load data in the cache.</p>
	 */
	RefResolver<Key, Value> getResolver();
	
	/**
	 * <p>Retrieve an element from the cache. If the element is already in the cache,
	 * it is simply returned. Otherwise, the <code>RefResolver</code> will be used to
	 * obtain it automatically from permanent storage.</p>
	 * 
	 * @param key The key of the element.
	 * @return The element's value.
	 */
	Value get(Key key);
	
	/**
	 * <p>Retrieve and return an element from the cache if it's already there or
	 * return <code>null</code> otherwise. This method will not call the 
	 * <code>RefResolver</code> when the element is not found in the cache.</p>
	 * 
	 * @param key The key of the element.
	 * @return The element's value or <code>null</code> is it's not found in the cache.
	 */
	Value getIfLoaded(Key key);
	
	/**
	 * <p>Return <code>true</code> if the element with the given key is currently in the
	 * cache and <code>false</code> otherwise.</p>
	 */
	boolean isLoaded(Key key);
	
	/**
	 * <p>Force removal of an element from the cache. This method is generally used
	 * when the data has been (or is being) removed from the permanent storage as well.
	 * </p>
	 * 
	 * @param key The key of the element.
	 */
	void remove(Key key);
	
	/**
	 * <p>Clear (i.e. force removal of) all elements from the cache.</p>
	 */
	void clear();
	
	/**
	 * <p>Return the number of elements currently in the cache.</p>
	 */
	int size();
}
 
