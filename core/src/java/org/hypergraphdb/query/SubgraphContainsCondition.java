package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.atom.HGSubgraph;

public class SubgraphContainsCondition implements HGQueryCondition, HGAtomPredicate
{
  private HGHandle atom;
  
  public SubgraphContainsCondition() { }
  public SubgraphContainsCondition(HGHandle atom) { this.atom = atom; }
  
  public boolean satisfies(HyperGraph graph, HGHandle subgHdl)
  {
    HGIndex<HGPersistentHandle, HGPersistentHandle> idx = 
        HGSubgraph.getReverseIndex(graph);
    HGRandomAccessResult<HGPersistentHandle> rs = idx.find(atom.getPersistent());
    try 
    {
    	return rs.goTo(subgHdl.getPersistent(), true) == GotoResult.found;
    }
    finally
    {
    	rs.close();
    }
  }
  
  public HGHandle getAtom()
  {
      return atom;
  }
  public void setAtom(HGHandle atom)
  {
      this.atom = atom;
  }

	public int hashCode() 
	{ 
		return atom.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SubgraphContainsCondition))
			return false;
		else
		{
			SubgraphContainsCondition c = (SubgraphContainsCondition)x;
			return atom.equals(c.getAtom());
		}
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("SubgraphDirectContainsCondition(");
		result.append("atom:");
		result.append(atom);
		result.append(")");
		return result.toString();
	}
}