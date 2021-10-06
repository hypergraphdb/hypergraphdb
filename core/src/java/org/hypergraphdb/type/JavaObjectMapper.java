/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.atom.HGSerializable;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.util.HGUtils;

public class JavaObjectMapper implements JavaTypeMapper
{
	protected HyperGraph graph = null;
	protected HashSet<String> classes = null;
	protected HGHandle superSlot = null;
	protected HGIndex<String, HGPersistentHandle> idx = null;
	
	protected HGIndex<String, HGPersistentHandle> getIndex()
	{
		if (idx == null)
		{
			HGHandle t = graph.getTypeSystem().getTypeHandle(HGSerializable.class);
			HGIndexer<String, HGPersistentHandle> indexer = new ByPartIndexer<String>(t, "classname");
			idx = graph.getIndexManager().getIndex(indexer);
			if (idx == null)
			{
				idx = graph.getIndexManager().register(indexer);
			}
			return idx;
		}
		return idx;
	}
	
	public HGHandle getSuperSlot()
	{
	    return JavaTypeFactory.getSuperSlot(graph);
	}	
	
	protected void initClasses()
	{
		if (classes != null)
			return;
		classes = new HashSet<String>();
		HGIndex<String, HGPersistentHandle> idx = getIndex();
		HGSearchResult<String> rs = idx.scanKeys();
		try
		{
			while (rs.hasNext())
				classes.add(rs.next());
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
		finally
		{
			HGUtils.closeNoException(rs);
		}
	}	
	
	protected boolean checkClass(Class<?> javaClass)
	{
		if (!classes.contains(javaClass.getName()))
		{
			Class<?> parent = javaClass.getSuperclass();
			if (parent == null)
				return false;
			if (checkClass(parent))
				return true;
			for (Class<?> in : javaClass.getInterfaces())
				if (checkClass(in))
					return true;
			return false;
		}
		else
			return true;
	}
	
	protected boolean mapAsSerializableObject(Class<?> javaClass)
	{
		initClasses();
		return checkClass(javaClass);
	}

	private boolean ignoreField(Field f)
	{
		int m = f.getModifiers();
		return (m & Modifier.TRANSIENT) != 0 ||
			   (m & Modifier.STATIC) != 0 ||
			   f.getAnnotation(HGIgnore.class) != null;
	}
	
	private HGAtomRef.Mode getReferenceMode(Class<?> javaClass, Field field)
	{
		//
		// Retrieve or recursively create a new type for the nested
		// bean.
		//
		AtomReference ann = (AtomReference)field.getAnnotation(AtomReference.class);
		if (ann == null)
			return null;
		String s = ann.value();
		if ("hard".equals(s))
			return HGAtomRef.Mode.hard;
		else if ("symbolic".equals(s))
			return HGAtomRef.Mode.symbolic;
		else if ("floating".equals(s))
			return HGAtomRef.Mode.floating;
		else
			throw new HGException("Wrong annotation value '" + s + 
					"' for field '" + field.getName() + "' of class '" +
					javaClass.getName() + "', must be one of \"hard\", \"symbolic\" or \"floating\".");
	}
	
	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}
	
	public HGAtomType defineHGType(Class<?> javaClass, HGHandle typeHandle)
	{
		if (!mapAsSerializableObject(javaClass))
			return null;
		
		HGTypeSystem typeSystem = graph.getTypeSystem();
//		JavaTypeMapper javaTypes = typeSystem.getJavaTypeFactory();
		Field fields [] = javaClass.getDeclaredFields();
				
		RecordType recType = new RecordType();

		// First, find out about the super slot which we put if the parent is a 
		// non-empty record.
		if (javaClass.getSuperclass() != null)
		{
			// Make sure we handle properly recursive calls - if the type handle yields
			// a 'Class' instance, it means we're in the process of constructing the type.
			// So we can simply check whether it has any fields that are not to be ignored.
			boolean has_parent = false;
			HGHandle parentTypeHandle = typeSystem.getTypeHandle(javaClass.getSuperclass());
			Object x = graph.get(parentTypeHandle);			
			if (x instanceof Class<?>)
			{
				Class<?> clazz = (Class<?>)x;				
				for (Field pf : clazz.getDeclaredFields())
					if (!ignoreField(pf)) { has_parent = true; break; }
			}
			else
			{
				HGAtomType parentType = typeSystem.getAtomType(javaClass.getSuperclass());
				has_parent = parentType instanceof HGCompositeType && 
							((HGCompositeType)parentType).getDimensionNames().hasNext();
			}
			if (has_parent)
				recType.addSlot(getSuperSlot());
		}
		
		for (Field field : fields)
		{ 
			if (ignoreField(field))
				continue;
			HGHandle fieldTypeHandle = typeSystem.getTypeHandle(field.getType());
			if (fieldTypeHandle == null)
			{
				throw new HGException("Unable to create HG type for field " + 
									  field.getName() + " of class " + javaClass.getName());
			}
			HGHandle slotHandle = JavaTypeFactory.getSlotHandle(graph, field.getName(), fieldTypeHandle);
			Slot slot = graph.get(slotHandle);			
			recType.addSlot(slotHandle);			
			HGAtomRef.Mode refMode = getReferenceMode(javaClass, field);						
			if (refMode != null)
				typeSystem.getHyperGraph().add(new AtomProjection(typeHandle, 
																  slot.getLabel(),
																  slot.getValueType(), 
																  refMode));			
		}
		
		if (recType.getSlots().isEmpty())
			return new HGAbstractType();
//		else if (!isInstantiable(javaTypes, javaClass))
//			return defineComposite(typeSystem, fields);
		else
			return recType;
	}

	public HGAtomType getJavaBinding(HGHandle typeHandle, 
									 HGAtomType hgType, 
									 Class<?> javaClass)
	{
		if (mapAsSerializableObject(javaClass))
		{
			if (hgType instanceof RecordType)
			{
				RecordType recType = (RecordType)hgType;
				recType.setThisHandle(typeHandle);
				if (JavaTypeFactory.isHGInstantiable(javaClass))
					return new JavaObjectBinding(typeHandle, recType, javaClass);
				else
					return new JavaAbstractBinding(typeHandle, recType, javaClass);
			}
			else if (hgType instanceof HGCompositeType)
				return new JavaAbstractBinding(typeHandle, (HGCompositeType)hgType, javaClass);
			
			else if (hgType instanceof HGAbstractType)
				return new JavaInterfaceBinding(typeHandle, hgType, javaClass);
			
			else
				return hgType;
		}
		return null;
	}
	
	public void addClass(Class<?> c)
	{
		addClass(c.getName());
	}
	
	public void addClass(String classname)
	{
		initClasses();
		try
		{
			Class<?> c = HGUtils.loadClass(graph, classname);
			for (String existing : classes)
			{				
				Class<?> e = null;
				try { e = Class.forName(existing); }
				catch (Exception ex) { }
				if (e != null && e.isAssignableFrom(c))
					return;
			}
			graph.add(new HGSerializable(classname));
			classes.add(classname);
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
	}
}
