package org.hypergraphdb.cache;

import org.hypergraphdb.util.RefResolver;

/**
 * 
 * <p>
 * A simple generic, read-only caching interface. An implementation must be initialized
 * by providing the means to load data that is not in the cache - a
 * <code>RefResolver</code> instance. Therefore, there's no <code>put</code>
 * method, the cache makes the decision if and when to actually keep data in it.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface HGCache<Key, Value>
{
	void setResolver(RefResolver<Key, Value> resolver);
	RefResolver<Key, Value> getResolver();
	Value get(Key key);	
}
 