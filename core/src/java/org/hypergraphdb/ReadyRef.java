package org.hypergraphdb;

/**
 * 
 * <p>
 * An implementation of <code>LazyRef</code> that simply encapsulates
 * an existing value. Use it to pass a known value as a parameter to a
 * method expecting a <code>LazyRef</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public final class ReadyRef<T> implements LazyRef<T> 
{
	private T value;
	
	public ReadyRef(T value)
	{
		this.value = value;
	}
	
	public T deref() 
	{
		return value;
	}
}