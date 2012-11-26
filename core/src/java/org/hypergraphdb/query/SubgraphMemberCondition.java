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
		if (subgraphHandle == null)
			throw new NullPointerException("Subgraph handle is null in SubgraphDirectMemberCondition!");
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
	  HGIndex<HGPersistentHandle, HGPersistentHandle> idx = HGSubgraph.getIndex(graph);
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
	
	public int hashCode() 
	{ 
		return subgraphHandle.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SubgraphMemberCondition))
			return false;
		else
		{
			SubgraphMemberCondition c = (SubgraphMemberCondition)x;
			return subgraphHandle.equals(c.getSubgraphHandle());
		}
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("SubgraphDirectMemberCondition(");
		result.append("subgraphHandle:");
		result.append(subgraphHandle);
		result.append(")");
		return result.toString();
	}
}