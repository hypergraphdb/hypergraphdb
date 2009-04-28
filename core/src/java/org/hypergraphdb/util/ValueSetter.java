package org.hypergraphdb.util;

/**
 * <p>
 * A simple interface to be implemented by objects that set values in specific
 * dynamic contexts. This is usual implemented by anonymous classes.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface ValueSetter<T>
{
	void set(T value);
}
