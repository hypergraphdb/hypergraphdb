/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;


import org.hypergraphdb.HGException;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGAtomTypeBase;
import org.hypergraphdb.type.HGPrimitiveType;

@SuppressWarnings("unchecked")
public class EnumType extends HGAtomTypeBase implements HGPrimitiveType
{
	private Class<Enum<?>> enumType;
	
	public EnumType()
	{		
	}
	
	public EnumType(Class<Enum<?>> enumType)
	{
		this.enumType = enumType;
	}
	
	public final Class<?> getEnumType()
	{
		return enumType;
	}

	public final void setEnumType(Class<Enum<?>> enumType)
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
		return Enum.valueOf((Class<Enum>)(Class)enumType, symbol);
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

    public Object fromByteArray(byte[] byteArray, int offset, int length)
    {
        int ordinal = BAUtils.readInt(byteArray, offset);
        return enumType.getEnumConstants()[ordinal];        
    }

    public byte[] toByteArray(Object object)
    {
        Enum<?> e = (Enum<?>)object;
        byte [] B = new byte[4];        
        BAUtils.writeInt(e == null? -1 : e.ordinal(), B, 0);
        return B;
    }
    
    public Comparator<byte[]> getComparator()
    {
    	return ENUM_COMPARATOR;
    }
    
    public static final class ENUM_COMPARATOR_IMPL implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] left, byte [] right)
	    {
	        for (int i = 0; i < left.length && i < right.length; i++)
	            if (left[i] - right[i] == 0)
	                continue;
	            else 
	                return left[i] - right[i];
	        return 0;        
	    }
    }
    
    public static final ENUM_COMPARATOR_IMPL ENUM_COMPARATOR = new ENUM_COMPARATOR_IMPL();
}