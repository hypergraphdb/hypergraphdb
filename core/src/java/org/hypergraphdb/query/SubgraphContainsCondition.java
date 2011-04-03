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
    
    public boolean satisfies(HyperGraph graph, HGHandle handle)
    {
        HGIndex<HGPersistentHandle, HGPersistentHandle> idx = 
            HGSubgraph.getReverseIndex(graph);
        HGRandomAccessResult<HGPersistentHandle> rs = idx.find(atom.getPersistent());
        try 
        {
            return rs.goTo(handle.getPersistent(), true) == GotoResult.found;
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
}