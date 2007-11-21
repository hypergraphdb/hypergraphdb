package org.hypergraphdb.util;

public final class CompositeMapping implements Mapping 
{
	private Mapping first;
	private Mapping second;
	
	public CompositeMapping(Mapping first, Mapping second)
	{
		this.first = first;
		this.second = second;
	}
	
	public Object eval(Object x) 
	{
		return second.eval(first.eval(x));		
	}
}