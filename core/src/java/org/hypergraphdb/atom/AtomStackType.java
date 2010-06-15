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

public class AtomStackType implements HGAtomType 
{
	private HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		HGAtomStack stack = new HGAtomStack();
		for (int i = 0; i < data.length; i += 16)
		{
			stack.push(hg.getHandleFactory().makeHandle(data, i));			
		}
		return stack;
	}

	public HGPersistentHandle store(Object instance) 
	{
		HGAtomStack stack = (HGAtomStack)instance;
		byte [] data = new byte[stack.size * 16];
		int i = (stack.size - 1)*16;
		for (U.HandleLink curr = stack.top; curr != null; curr = curr.next, i-= 16)
			System.arraycopy(U.getBytes(curr.handle), 0, data, i, 16);
		return hg.getStore().store(data);
	}

	public void release(HGPersistentHandle handle) 
	{
		hg.getStore().removeData(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return general == specific;
	}
}
