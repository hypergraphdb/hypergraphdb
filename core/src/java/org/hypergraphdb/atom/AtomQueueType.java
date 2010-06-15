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

public class AtomQueueType implements HGAtomType 
{
	private HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		HGAtomQueue queue = new HGAtomQueue();
		byte [] data = hg.getStore().getData(handle);
		for (int i = 0; i < data.length; i += 16)
		{
			queue.enqueue(hg.getHandleFactory().makeHandle(data, i));			
		}		
		return queue;
	}

	public HGPersistentHandle store(Object instance) 
	{
		HGAtomQueue queue = (HGAtomQueue)instance;
		byte [] data = new byte[queue.size*16];		
		int i = (queue.size - 1)*16;
		for (U.HandleLink curr = queue.front; curr != null; curr = curr.next, i -= 16)
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
