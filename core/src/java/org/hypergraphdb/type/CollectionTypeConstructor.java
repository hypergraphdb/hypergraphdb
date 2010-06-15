/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * Represents the type of <code>CollectionType</code>s. Records the concrete type of
 * a collection in a rather unportable way - by storing the fully-qualified class name. 
 * It is not obvious at all how to represent concrete Java types in a portable way. 
 * Each concrete collection implementation has a specific behavior that may or may
 * not be immitatable in another run-time environment.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class CollectionTypeConstructor implements HGAtomType 
{
	private HyperGraph graph;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.graph = hg;
	}

	@SuppressWarnings("unchecked")
    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		CollectionType result = null;
		String className = new String(graph.getStore().getData(handle));
		try
		{
			Class<?> clazz = Class.forName(className);
			GenericObjectFactory factory = new GenericObjectFactory(clazz);
			result = new CollectionType(factory);
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		CollectionType type = (CollectionType)instance;
		String className = type.getFactory().getType().getName();
		HGPersistentHandle result = graph.getStore().store(className.getBytes());
		return result;
	}

	public void release(HGPersistentHandle handle) 
	{
		graph.getStore().removeData(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}
