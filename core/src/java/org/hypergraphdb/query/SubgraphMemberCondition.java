package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGSubgraph;

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
	    HGIndex<HGPersistentHandle, HGPersistentHandle> idx = 
	        HGSubgraph.getIndex(graph);
		HGRandomAccessResult<HGPersistentHandle> rs = idx.find(subgraphHandle.getPersistent());
		try 
		{
		    return rs.goTo(handle.getPersistent(), true) == GotoResult.found;
		}
		finally
		{
		    rs.close();
		}
	}	
}