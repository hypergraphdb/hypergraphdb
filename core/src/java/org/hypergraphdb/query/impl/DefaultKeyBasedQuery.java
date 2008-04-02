package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.ValueSetter;

public class DefaultKeyBasedQuery extends KeyBasedQuery 
{
	private HyperGraph graph;
	private HGQueryCondition cond;
	private ValueSetter setter;
	
	public DefaultKeyBasedQuery(HyperGraph graph, HGQueryCondition cond, ValueSetter setter)
	{
		this.graph = graph;
		this.cond = cond;
		this.setter = setter;
	}
	
	@Override
	public Object getKey() 
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setKey(Object key) 
	{
		setter.set(key);
	}

	@Override
	public HGSearchResult<?> execute() 
	{		
		return graph.find(cond);
	}
}