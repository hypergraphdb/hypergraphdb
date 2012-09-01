package org.hypergraphdb.util;

public class Constant<T> implements Ref<T>
{
	private T x;
	
	public Constant(T x)  { this.x = x;}
	
	public T get() { return x; }
	
	public String toString()
	{
		return x == null ? "null" : x.toString();
	}
	
	public int hashCode()
	{
		return x == null ? 0 : x.hashCode();
	}
	
	public boolean equals(Object y)
	{
		if (! (y instanceof Constant))
			return false;
		@SuppressWarnings("unchecked")
		Constant<T> c = (Constant<T>)y;
		if (x == null)
			return c.x == null;
		else if (c.x == null)
			return false;
		else
			return x.equals(c.x);
	}
}