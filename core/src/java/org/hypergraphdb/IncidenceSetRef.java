package org.hypergraphdb;

/**
 * <p>
 * An implementation that will lazily query for the incidence set
 * of an atom. Many <code>HGAtomType</code>s ignore the incidence
 * set when constructing the value of a given atom. For this reason,
 * it is only loaded on demand since it requires an extra DB query.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class IncidenceSetRef implements LazyRef<HGHandle[]> 
{
	private HGHandle [] set = null;
	private HGHandle atomHandle;
	private HyperGraph graph;
	
	public IncidenceSetRef(HGHandle atomHandle, HyperGraph graph)
	{
		this.atomHandle = atomHandle;
		this.graph = graph;
	}
	
	public HGHandle [] deref() 
	{
		if (set == null)
			set = graph.getIncidenceSet(atomHandle);
		return set;
	}
}