package org.hypergraphdb.type;

import java.lang.reflect.Field;
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
	private HyperGraph graph = null;
	private HashSet<String> classes = null;
	private HGHandle superSlot = null;
	private HGIndex<String, HGPersistentHandle> idx = null;
	
	private HGIndex<String, HGPersistentHandle> getIndex()
	{
		if (idx == null)
		{
			HGHandle t = graph.getTypeSystem().getTypeHandle(HGSerializable.class);
			HGIndexer indexer = new ByPartIndexer(t, "classname");
			idx = graph.getIndexManager().getIndex(indexer);
			if (idx == null)
			{
				idx = graph.getIndexManager().register(indexer);
			}
			return idx;
		}
		return idx;
	}
	
	private HGHandle getSuperSlot()
	{
		if (superSlot == null)
			superSlot = graph.getTypeSystem().getJavaTypeFactory().getSlotHandle(
					"!super", graph.getTypeSystem().getTypeHandle(HGPersistentHandle.class));
		return superSlot;
	}	
	
	private void initClasses()
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
	
	private boolean checkClass(Class<?> javaClass)
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
	
	private boolean mapAsSerializableObject(Class<?> javaClass)
	{
		initClasses();
		return checkClass(javaClass);
/*		for (String existing : classes)
		{				
			Class<?> e = null;
			try { e = Class.forName(existing); }
			catch (Exception ex) { }
			if (e != null && e.isAssignableFrom(javaClass))
				return true;
		}		
		return false; */ 
	}

	private HGAtomType defineComposite(HGTypeSystem typeSystem, Field [] fields) 
	{
		HGAbstractCompositeType compositeType = new HGAbstractCompositeType();
		for (Field f : fields)
		{
			compositeType.addProjection(
				new HGAbstractCompositeType.Projection(
						f.getName(), 
						typeSystem.getTypeHandle(f.getType())));
		}
		return compositeType;
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
		JavaTypeFactory javaTypes = typeSystem.getJavaTypeFactory();
		
		boolean is_abstract = javaTypes.isAbstract(javaClass);
		boolean is_default_constructible = javaTypes.isDefaultConstructible(javaClass);
		boolean is_link = javaTypes.isLink(javaClass);		
		
		Field fields [] = javaClass.getDeclaredFields();
		
		if (is_abstract || !(is_default_constructible || is_link))
			return defineComposite(typeSystem, fields);
		
		RecordType recType = new RecordType();
		for (Field field : fields)
		{ 
			if (field.getAnnotation(HGIgnore.class) != null)
				continue;
			HGHandle slotHandle = javaTypes.getSlotHandle(field.getName(), 
														  typeSystem.getTypeHandle(field.getType()));
			recType.addSlot(slotHandle);
			HGAtomRef.Mode refMode = getReferenceMode(javaClass, field);						
			if (refMode != null)
				typeSystem.getHyperGraph().add(new AtomProjection(typeHandle, slotHandle, refMode));			
		}
		
		if (javaClass.getSuperclass() != null)
		{
			HGAtomType parentType = typeSystem.getAtomType(javaClass.getSuperclass());
			// Store super class only if its type is a non-empty composite type.
			if (parentType instanceof HGCompositeType && 
				((HGCompositeType)parentType).getDimensionNames().hasNext())
				recType.addSlot(getSuperSlot());
		}
		if (recType.getSlots().isEmpty())
			return new HGAbstractType();
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
				return new JavaBeanBinding(typeHandle, (RecordType)hgType, javaClass);
			else if (hgType instanceof HGCompositeType)
				return new JavaAbstractBeanBinding(typeHandle, (HGCompositeType)hgType, javaClass);
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
			Class<?> c = Class.forName(classname);
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