/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomType;

/**
 * <p>
 * Implements the HyperGraph type of <code>HGAtomSet</code> atoms.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class AtomSetType implements HGAtomType 
{
	private HyperGraph graph;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.graph = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = graph.getStore().getData(handle);
		HGAtomSet set = new HGAtomSet();
		for (int  pos = 0; pos < data.length; pos += 16)
			set.add(graph.getHandleFactory().makeHandle(data, pos));
		return set;
	}

	public HGPersistentHandle store(Object instance) 
	{
		HGAtomSet set = (HGAtomSet)instance;
		HGPersistentHandle result = graph.getHandleFactory().makeHandle();
		byte [] A = new byte[set.size()*16];
		int pos = 0;
		for (HGHandle h:set)
		{
			System.arraycopy(graph.getPersistentHandle(h).toByteArray(), 0, A, pos, 16);
			pos += 16;
		}
		graph.getStore().store(result, A);
		return result;
	}

	public void release(HGPersistentHandle handle) 
	{
		graph.getStore().removeData(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return general == specific;
	}
}