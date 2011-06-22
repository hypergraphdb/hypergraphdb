/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Map;
import java.util.Iterator;

import org.hypergraphdb.HGEMissingData;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

public class MapType implements HGAtomType 
{
	private HyperGraph hg;
	private ObjectFactory<Map<Object, Object>> factory = null;
	
	public MapType(ObjectFactory<Map<Object, Object>> factory)
	{
		this.factory = factory;
	}
	
	public ObjectFactory<?> getFactory()
	{
		return factory;
	}
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		Map<Object, Object> result;
		if (targetSet == null || targetSet.deref().length == 0)
			result = factory.make();
		else
			result = factory.make(targetSet.deref());
		TypeUtils.setValueFor(hg, handle, result);
		HGTypeSystem ts = hg.getTypeSystem();
		HGPersistentHandle [] layout = hg.getStore().getLink(handle);
		if (layout == null)
			throw new HGEMissingData(handle);
		for (int i = 0; i < layout.length; )
		{
			Object key, value;
			HGPersistentHandle hType = layout[i++];
			HGAtomType type = ts.getType(hType);
			HGPersistentHandle hValue = layout[i++];
			key = TypeUtils.makeValue(hg, hValue, type);
			hType = layout[i++];
			hValue = layout[i++];
			if (hType.equals(hg.getHandleFactory().nullHandle()))
				result.put(key, null);
			else
			{
				type = ts.getType(hType);
				value = TypeUtils.makeValue(hg, hValue, type);
				result.put(key, value);
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
    public HGPersistentHandle store(Object instance) 
	{
		HGPersistentHandle result = TypeUtils.getNewHandleFor(hg, instance);
		Map map = (Map)instance;
		HGPersistentHandle [] layout = new  HGPersistentHandle[map.size()*4];
		int pos = 0;
		for (Iterator i = map.entrySet().iterator(); i.hasNext(); )
		{
			Map.Entry entry = (Map.Entry)i.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			HGHandle typeHandle = hg.getTypeSystem().getTypeHandle(key.getClass());
			if (typeHandle == null)
			    throw new HGException("Unable to get HG type for class '" + key.getClass() + "'");
			layout[pos++] = hg.getPersistentHandle(typeHandle);
			layout[pos++] = TypeUtils.storeValue(hg, key, hg.getTypeSystem().getType(typeHandle));
			if (value == null)
			{
				layout[pos++] = hg.getHandleFactory().nullHandle();
				layout[pos++] = hg.getHandleFactory().nullHandle();
			}
			else
			{
				typeHandle = hg.getTypeSystem().getTypeHandle(value.getClass());
	            if (typeHandle == null)
	                throw new HGException("Unable to get HG type for class '" + value.getClass() + "'");			
				layout[pos++] = hg.getPersistentHandle(typeHandle);
				layout[pos++] = TypeUtils.storeValue(hg, value, hg.getTypeSystem().getType(typeHandle));
			}
		}		
		return hg.getStore().store(result, layout); 
	}

	public void release(HGPersistentHandle handle) 
	{
//		TypeUtils.releaseValue(hg, handle);
		HGTypeSystem ts = hg.getTypeSystem();
		HGPersistentHandle [] layout = hg.getStore().getLink(handle);
		for (int i = 0; i < layout.length; )
		{
			HGPersistentHandle hType = layout[i++];
			HGPersistentHandle hValue = layout[i++];
			if (!TypeUtils.isValueReleased(hg, hValue))
			{
			    HGAtomType type = ts.getType(hType); 
				TypeUtils.releaseValue(hg, type, hValue);
				//type.release(hValue);
			}
			hType = layout[i++];
			hValue = layout[i++];
			if (!hType.equals(hg.getHandleFactory().nullHandle()) && 
				!TypeUtils.isValueReleased(hg, hValue))
			{
			    HGAtomType type = ts.getType(hType);
				TypeUtils.releaseValue(hg, type, hValue);
				//type.release(hValue);
			}
		}
		hg.getStore().removeLink(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}
