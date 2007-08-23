/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HyperGraph;
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
	private HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		HGAtomSet set = new HGAtomSet();
		set.trie.deserialize(data);
		return set;
	}

	public HGPersistentHandle store(Object instance) 
	{
		HGAtomSet set = (HGAtomSet)instance;
		HGPersistentHandle result = HGHandleFactory.makeHandle(); 
		hg.getStore().store(result, set.trie.serialize());
		return result;
	}

	public void release(HGPersistentHandle handle) 
	{
		hg.getStore().remove(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return general == specific;
	}
}
