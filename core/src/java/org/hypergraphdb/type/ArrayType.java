/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * An <code>ArrayType</code> handles fixed size arrays of values of the same
 * type (i.e. having the same <code>HGAtomType</code>). Fixed size means that
 * once an array is created with a given size, subsequent reads and updates
 * assumes exactly that size.
 * </p>
 * 
 * <p>
 * Arrays may be of size 0, or even <code>null</code>. Null arrays are
 * recorded by simply storing a <code>HGHandleFactory.nullHandle</code>. All
 * other arrays (including 0 sized) are recorded by storing the handle of the
 * element type, then a prototypical, default constructed value for the sole
 * purpose of recovering the actual Java type (we avoid storing classnames since
 * they are not portable), followed by each element of the array.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ArrayType implements HGAtomType
{
	private HyperGraph graph = null;

	private Constructor<?> linkConstructor = null;

	private Class<?> clazz;

	public ArrayType(Class<?> clazz)
	{
		this.clazz = clazz;
		try
		{
			linkConstructor = clazz
					.getDeclaredConstructor(new Class[] { HGHandle[].class });
		} 
		catch (NoSuchMethodException ex)
		{
		}
	}

	public Class<?> getType()
	{
		return clazz;
	}

	public void setHyperGraph(HyperGraph hg)
	{
		this.graph = hg;
	}

	public Object make(HGPersistentHandle handle,
					   LazyRef<HGHandle[]> targetSet, 
					   IncidenceSetRef incidenceSet)
	{
		HGPersistentHandle[] layout = graph.getStore().getLink(handle);
		Object result;
		if (targetSet == null || targetSet.deref().length == 0)
			result = Array.newInstance(clazz, layout.length / 2);
		else
		{
			if (linkConstructor == null)
				throw new HGException(
						"Unable to construct a link of type "
								+ clazz.getName()
								+ ", the class doesn't have a HGHandle [] based constructor.");
			try
			{
				result = linkConstructor.newInstance(new Object[] { targetSet });
			} 
			catch (Throwable t)
			{
				throw new HGException(t);
			}
		}
		TypeUtils.setValueFor(graph, handle, result);
		for (int i = 0; i < layout.length; i += 2)
		{
			Object current = null;
			HGPersistentHandle typeHandle = layout[i];
			HGPersistentHandle valueHandle = layout[i + 1];
			if (!typeHandle.equals(graph.getHandleFactory().nullHandle()))
			{
				HGAtomType type = graph.getTypeSystem().getType(typeHandle);
				current = TypeUtils.makeValue(graph, valueHandle, type);
			}
			((Object[])result)[i / 2] = current;
		}
		return result;
	}

	public HGPersistentHandle store(Object instance)
	{
		HGPersistentHandle result = TypeUtils.getNewHandleFor(graph, instance);
		Object[] array = (Object[]) instance;
		HGPersistentHandle[] layout = new HGPersistentHandle[array.length * 2];
		int pos = 0;
		for (int i = 0; i < array.length; i++)
		{
			Object curr = array[i];
			if (curr == null)
			{
				layout[pos++] = graph.getHandleFactory().nullHandle();
				layout[pos++] = graph.getHandleFactory().nullHandle();
			} else
			{
				HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(
						curr.getClass());
				layout[pos++] = graph.getPersistentHandle(typeHandle);
				layout[pos++] = TypeUtils.storeValue(graph, curr, graph
						.getTypeSystem().getType(typeHandle));
			}
		}
		graph.getStore().store(result, layout);
		return result;
	}

	public void release(HGPersistentHandle handle)
	{
		// TypeUtils.releaseValue(hg, handle);
		HGPersistentHandle[] layout = graph.getStore().getLink(handle);
		for (int i = 0; i < layout.length; i += 2)
		{
			HGPersistentHandle typeHandle = layout[i];
			HGPersistentHandle valueHandle = layout[i + 1];
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
