/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

/**
 * 
 * <p>
 * Represents the type a "semantic" relationship. It carries the
 * name of the relationships and the types of its arguments. The latter
 * are simply the target set of a <code>HGRelType</code> instance. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGRelType extends HGAtomTypeBase implements HGLink 
{
	private String name;
	private HGHandle [] targetTypes;
	
	private void init(String name, HGHandle...targetTypes)
	{
		this.name = name;
		if (targetTypes == null)
			targetTypes = HyperGraph.EMPTY_HANDLE_SET;
		this.targetTypes = targetTypes;		
	}
	
	public HGRelType()
	{
		init("");
	}
	
	public HGRelType(String name)
	{
		init(name);
	}
	
	public HGRelType(HGHandle...targetTypes)
	{
		init("", targetTypes);
	}
	
	public HGRelType(String name, HGHandle...targetTypes)
	{
		init(name, targetTypes);
	}
	
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		if (targetSet == null)
		{
			if (targetTypes != null && targetTypes.length > 0)
				throw new HGException("HGRelType.make: expected a target set of size " + 
						targetTypes.length +  " but got a null.");
		}
		else if (targetSet.deref().length != targetTypes.length)
			throw new HGException("HGRelType.make: expected a target set of size " + 
					targetTypes.length + " but got size " + targetSet.deref().length);		
		return new HGRel(name, targetSet.deref());
	}

	public void release(HGPersistentHandle handle) 
	{
	}

	public HGPersistentHandle store(Object instance) 
	{
		// there's no value of the relation per se, it's only a link
		// that's simply strongly typed....and we don't bother checking
		// the types of the target atoms for now
		return graph.getHandleFactory().nullHandle();
	}

	public String getName() 
	{
		return name;
	}

	public void setName(String name) 
	{
		this.name = name;
	}

	public int getArity() 
	{
		return targetTypes.length;
	}

	public HGHandle getTargetAt(int i) 
	{
		return targetTypes[i];
	}

	public void notifyTargetHandleUpdate(int i, HGHandle handle) 
	{
		targetTypes[i] = handle;
	}
	
    public void notifyTargetRemoved(int i)
    {
    	HGHandle [] newOutgoing = new HGHandle[targetTypes.length - 1];
    	System.arraycopy(targetTypes, 0, newOutgoing, 0, i);
    	System.arraycopy(targetTypes, i + 1, newOutgoing, i, targetTypes.length - i -1);
    	targetTypes = newOutgoing;
    }
    
	public int hashCode()
	{
		return name == null ? 0 : name.hashCode(); 
	}
	
	public boolean equals(Object other)
	{
		if (! (other instanceof HGRelType))
			return false;
		HGRelType rt = (HGRelType)other;
		if (!rt.getName().equals(name))
			return false;
		else if (targetTypes == rt.targetTypes)
			return true;
		else if (targetTypes.length != rt.targetTypes.length)
			return false;		
		for (int i = 0; i < targetTypes.length; i++)
			if (!targetTypes[i].equals(rt.targetTypes[i]))
				return false;
		return true;			
	}
}
