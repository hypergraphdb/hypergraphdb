package org.hypergraphdb.util;

public final class CompositeMapping<From, To> implements Mapping<From, To> 
{
	private Mapping<From, Object> first;
	private Mapping<Object, To> second;
	
	public CompositeMapping(Mapping<From, Object> first, Mapping<Object, To> second)
	{
		this.first = first;
		this.second = second;
	}
	
	public To eval(From x) 
	{
		return second.eval(first.eval(x));		
	}
}