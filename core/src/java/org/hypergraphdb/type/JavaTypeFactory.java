/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.AccessibleObjectCache;


/**
 * 
 * <p>
 * The <code>JavaTypeFactory</code> is used to infer HyperGraph types based on
 * Java beans. Java bean classes are converted to instances of
 * <code>RecordType</code> and Java bean instances to the corresponding
 * <code>Record</code>s. Only properties that are both readable and writeable
 * are mapped to HyperGraph <code>Slot</code>s.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class JavaTypeFactory implements JavaTypeMapper
{
	protected HyperGraph graph;
	private AccessibleObjectCache accessibleObjectCache = new AccessibleObjectCache();
	private ArrayList<JavaTypeMapper> mappers = new ArrayList<JavaTypeMapper>();
	private JavaObjectMapper objectMapper = null;
	private DefaultJavaTypeMapper defaultMapper = null;
	
	public JavaTypeFactory()
	{
		mappers.add(defaultMapper = new DefaultJavaTypeMapper());		
	}

	public void initNonDefaultMappers()
	{
		objectMapper = new JavaObjectMapper();
		objectMapper.setHyperGraph(graph);
		mappers.add(0, objectMapper);
	}
	
	public void assign(Object bean, String property, Object value) 
	{
		BonesOfBeans.setProperty(bean, property, value);
	}

	public void assignPrivate(Class<?> clazz, Object x, String field, Object value)
	{
		Field f = accessibleObjectCache.getField(clazz, field);
		try
		{
			f.set(x, value);
		}
		catch (IllegalAccessException ex)
		{
			throw new HGException(ex);
		}
	}
	
	public Object retrievePrivate(Class<?> clazz, Object x, String field)
	{
		Field f = accessibleObjectCache.getField(x.getClass(), field);
		try
		{
			return f.get(x);
		}
		catch (IllegalAccessException ex)
		{
			throw new HGException(ex);
		}
	}

	public static boolean isAbstract(Class<?> c)
	{
		return Modifier.isAbstract(c.getModifiers()) || 
		  	   Modifier.isInterface(c.getModifiers());
	}
	
	public static boolean isDefaultConstructible(Class<?> c)
	{
		try 
		{
			c.getConstructor(new Class[0]);
			return true;
		}
		catch (NoSuchMethodException ex) 
		{
			return false;
		}		
	}
	
	public static boolean isLink(Class<?> c)
	{
		try 
		{
			c.getDeclaredConstructor(new Class[] { HGHandle[].class });
			return HGLink.class.isAssignableFrom(c);
		}
		catch (NoSuchMethodException ex) 
		{
			return false;
		}		
	}
	
	public static boolean isHGInstantiable(Class<?> c)
	{
		return !isAbstract(c) && (isDefaultConstructible(c) || isLink(c));
	}
	
	public HGHandle getSlotHandle(String label, HGHandle type)
	{
		HGHandle slotHandle = hg.findOne(graph, 
				 						 hg.eq(new Slot(label,type)));
		if (slotHandle == null)
			return graph.add(new Slot(label, type));
		else
			return slotHandle;
	}
	
	public HGAtomType defineHGType(Class<?> javaClass, HGHandle typeHandle)
	{
		for (JavaTypeMapper m : mappers)
		{
			HGAtomType t = m.defineHGType(javaClass, typeHandle);
			if (t != null)
				return t;
		}
		return null;
	}

	public HGAtomType getJavaBinding(HGHandle typeHandle, HGAtomType hgType, Class<?> javaClass)
	{
		for (JavaTypeMapper m : mappers)
		{
			HGAtomType t = m.getJavaBinding(typeHandle, hgType, javaClass);
			if (t != null)
				return t;
		}
		return null;
	}	
	
	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
		for (JavaTypeMapper m : mappers)
			m.setHyperGraph(graph);
	}	
	
	public DefaultJavaTypeMapper getDefaultJavaMapper()
	{
		return defaultMapper;
	}
	
	public JavaObjectMapper getJavaObjectMapper()
	{
		return objectMapper;
	}
	
	public List<JavaTypeMapper> getMappers()
	{
		return mappers;
	}
}