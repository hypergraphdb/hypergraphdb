package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Mapping;

public class DerefMapping implements Mapping
{
	private HyperGraph graph;
	
	public DerefMapping(HyperGraph graph)
	{
		this.graph = graph;		 
	}
	
	public Object eval(Object x)
	{		
		return graph.get((HGHandle)x);
	}
	
	public HyperGraph getGraph()
	{
		return graph;
	}
}