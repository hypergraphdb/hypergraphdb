package org.hypergraphdb.util;

/**
 * 
 * <p>
 * Defines a generic capability to resolve a reference of type <code>Key</code> to an
 * object of type <code>Value</code>. This is similar to a {@link LazyRef} but
 * with a key.
 * </p>
 *
 * @author Borislav Iordanov
 */
public interface RefResolver<Key, Value>
{
	Value resolve(Key key);
}