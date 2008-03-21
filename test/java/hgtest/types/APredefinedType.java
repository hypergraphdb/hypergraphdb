package hgtest.types;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomType;

public class APredefinedType implements HGAtomType 
{
	public Object make(HGPersistentHandle handle,
			LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		return "APredefined instance";
	}

	public void release(HGPersistentHandle handle) 
	{
	}

	public void setHyperGraph(HyperGraph hg) 
	{
	}

	public HGPersistentHandle store(Object instance) 
	{
		return HGHandleFactory.nullHandle();
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}