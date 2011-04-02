package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

public class SubgraphMemberCondition implements HGQueryCondition, HGAtomPredicate
{
	private HGHandle subgraphHandle;
	
	public SubgraphMemberCondition()
	{		
	}
	
	public SubgraphMemberCondition(HGHandle subgraphHandle)
	{
		this.subgraphHandle = subgraphHandle;
	}

	public HGHandle getSubgraphHandle()
	{
		return subgraphHandle;
	}

	public void setSubgraphHandle(HGHandle subgraphHandle)
	{
		this.subgraphHandle = subgraphHandle;
	}

	public boolean satisfies(HyperGraph graph, HGHandle handle)
	{
		// TODO...
		return false;
	}	
}