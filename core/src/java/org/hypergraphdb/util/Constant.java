package org.hypergraphdb.util;

public class Constant<T> implements Ref<T>
{
	private T x;
	
	public Constant(T x)  { this.x = x;}
	
	public T get() { return x; }
}
