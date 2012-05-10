package hgtest.types;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomType;

public class APredefinedType implements HGAtomType 
{
    private HyperGraph graph;
    
	public Object make(HGPersistentHandle handle,
			LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		return "APredefined instance";
	}

	public void release(HGPersistentHandle handle) 
	{
	}

	public void setHyperGraph(HyperGraph graph) 
	{
		this.graph = graph;
	}

	public HGPersistentHandle store(Object instance) 
	{
		return graph.getHandleFactory().nullHandle();
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}