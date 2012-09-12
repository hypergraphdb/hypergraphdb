/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
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
public class IncidenceSetRef implements LazyRef<IncidenceSet> 
{
	private IncidenceSet set = null;
	private HGHandle atomHandle;
	private HyperGraph graph;
	
	public IncidenceSetRef(HGHandle atomHandle, HyperGraph graph)
	{
		this.atomHandle = atomHandle;
		this.graph = graph;
	}
	
	public IncidenceSet deref() 
	{
		if (set == null)
			set = graph.getIncidenceSet(atomHandle);
		return set;
	}

	public HGHandle getAtomHandle() {
		return atomHandle;
	}
}
