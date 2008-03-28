package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Mapping;

public final class DerefMapping implements Mapping<HGHandle, Object>
{
	private HyperGraph graph;
	
	public DerefMapping(HyperGraph graph)
	{
		this.graph = graph;		 
	}
	
	public Object eval(HGHandle x)
	{		
		return graph.get(x);
	}
	
	public HyperGraph getGraph()
	{
		return graph;
	}
}