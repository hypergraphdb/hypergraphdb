/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Collection;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * A <code>CollectionType</code> instance is capable for managing collections
 * of objects in the HyperGraph store. Nothing is assumed about the elements in the
 * collection. In particular, the latter may be heterogenous. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class CollectionType implements HGAtomType 
{
	private HyperGraph graph = null;
	private ObjectFactory<Collection<Object>> factory = null;
	
	public CollectionType(ObjectFactory<Collection<Object>> factory)
	{
		this.factory = factory;
	}
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.graph = hg;
	}

	public ObjectFactory<Collection<Object>> getFactory()
	{
		return factory;
	}
	
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		Collection<Object> result;
		if (targetSet == null || targetSet.deref().length == 0)
			result = factory.make();
		else
			result = factory.make(targetSet.deref());
		TypeUtils.setValueFor(graph, handle, result);
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		for (int i = 0; i < layout.length; i += 2)
		{
			Object current = null;			
			HGPersistentHandle typeHandle = layout[i];
			HGPersistentHandle valueHandle = layout[i+1];
			if (!typeHandle.equals(graph.getHandleFactory().nullHandle()))
			{
				HGAtomType type = graph.getTypeSystem().getType(typeHandle); 
				current = TypeUtils.makeValue(graph, valueHandle, type);
			}
			result.add(current);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public HGPersistentHandle store(Object instance) 
	{
		HGPersistentHandle result = TypeUtils.getNewHandleFor(graph, instance);
		Collection<Object> collection = (Collection<Object>)instance;
		HGPersistentHandle [] layout = new HGPersistentHandle[collection.size()*2];
		int pos = 0;
		for (Object curr : collection)
		{
			if (curr == null)
			{
				layout[pos++] = graph.getHandleFactory().nullHandle();
				layout[pos++] = graph.getHandleFactory().nullHandle();
			}
			else
			{
				HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(curr.getClass());
				layout[pos++] = graph.getPersistentHandle(typeHandle);
				layout[pos++] = TypeUtils.storeValue(graph, 
													 curr, 
													 graph.getTypeSystem().getType(typeHandle));
			}
		}
		graph.getStore().store(result, layout);
		return result;
	}

	public void release(HGPersistentHandle handle) 
	{
//		TypeUtils.releaseValue(hg, handle);
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		for (int i = 0; i < layout.length; i += 2)
		{		
			HGPersistentHandle typeHandle = layout[i];
			HGPersistentHandle valueHandle = layout[i+1];
            if (typeHandle.equals(graph.getHandleFactory().nullHandle()))
                continue;           			
			if (!TypeUtils.isValueReleased(graph, valueHandle))
			{
			    HGAtomType type = graph.get(typeHandle);
				TypeUtils.releaseValue(graph, type, valueHandle);
				//type.release(valueHandle);
			}
		}
		graph.getStore().removeLink(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}
