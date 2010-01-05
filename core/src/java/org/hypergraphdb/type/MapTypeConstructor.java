/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

public class MapTypeConstructor implements HGAtomType
{
	private HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		MapType result = null;
		String className = new String(hg.getStore().getData(handle));
		try
		{
			Class clazz = Class.forName(className);
			GenericObjectFactory factory = new GenericObjectFactory(clazz);
			result = new MapType(factory);
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		MapType type = (MapType)instance;
		String className = type.getFactory().getType().getName();
		HGPersistentHandle result = hg.getStore().store(className.getBytes());
		return result;
	}

	public void release(HGPersistentHandle handle) 
	{
		hg.getStore().removeData(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}
