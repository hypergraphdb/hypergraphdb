package org.hypergraphdb.type.javaprimitive;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGAtomTypeBase;

@SuppressWarnings("unchecked")
public class EnumType extends HGAtomTypeBase
{
	private Class<Enum> enumType;
	
	public EnumType()
	{		
	}
	
	public EnumType(Class<Enum> enumType)
	{
		this.enumType = enumType;
	}
	
	public final Class<?> getEnumType()
	{
		return enumType;
	}

	public final void setEnumType(Class<Enum> enumType)
	{
		this.enumType = enumType;
	}
	
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{
		// ignore target set...
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		if (layout == null || layout.length != 1)
			throw new HGException("EnumType.make: wrong or inexisting layout for handle " + 
			                      handle);
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		String symbol = (String)stringType.make(layout[0], null, null);
		return Enum.valueOf((Class<Enum>)enumType, symbol);
	}

	public HGPersistentHandle store(Object instance)
	{
		Enum<?> e = (Enum<?>)instance;
		if (!e.getClass().equals(enumType))
			throw new HGException("Attempting to store an enum instance of the wrong type " + 
					e.getClass().getName() + ", expected " + enumType.getName());
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		HGPersistentHandle [] layout = new HGPersistentHandle[1];
		layout[0] = stringType.store(e.name());
		return graph.getStore().store(layout);
	}

	public void release(HGPersistentHandle handle)
	{
		HGPersistentHandle [] layout = graph.getStore().getLink(handle);
		if (layout == null || layout.length != 1)
			throw new HGException("EnumType.release: wrong or inexisting layout for handle " + handle);		
		HGAtomType stringType = graph.getTypeSystem().getAtomType(String.class);
		stringType.release(layout[0]);
		graph.getStore().removeLink(handle);
	}
}