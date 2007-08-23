/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.LazyRef;

import java.io.*;
//import java.lang.reflect.Modifier;

/**
 * 
 * <p>
 * This type implementation handles values as serializable Java blobs. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class SerializableType implements HGAtomType
{
	private HyperGraph hg;
/*	private Class clazz; */

	public SerializableType(/* Class clazz */)
	{
/*		if (clazz == null)
			throw new NullPointerException("Attempt to construct a 'SerializableType' with a null Java class.");
		else if (!Serializable.class.isAssignableFrom(clazz))
			throw new IllegalArgumentException("Attempt to a construct a HyperGraph SerializableType from a non-serializable Java class.");
		else if (Modifier.isAbstract(clazz.getModifiers()))
			throw new IllegalArgumentException("Attempt to a construct a HyperGraph SerializableType from an abstract Java class."); 
		this.clazz = clazz; */
	}
	
	public void setHyperGraph(HyperGraph hg)
	{	
		this.hg = hg;
	}
	
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet)
	{
		try
		{
			ByteArrayInputStream in = new ByteArrayInputStream(hg.getStore().getData(handle));
			ObjectInputStream objectIn = new ObjectInputStream(in);		
			return objectIn.readObject();
		}
		catch (Exception ex)
		{
			throw new HGException(ex);
		}
	}
	
	public void release(HGPersistentHandle handle)
	{
		hg.getStore().remove(handle);
	}
	
	public HGPersistentHandle store(Object instance)
	{
/*		if (clazz.isAssignableFrom(instance.getClass()))
			throw new IllegalArgumentException("Object of type " + 
					instance.getClass().getName() + " is not an instance of " + clazz.getName()); */
		try
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream objectOut = new ObjectOutputStream(out);
			objectOut.writeObject(instance);		
			objectOut.flush();
			return hg.getStore().store(out.toByteArray());
		}
		catch (IOException ex)
		{
			throw new HGException(ex);
		}
	}
	
	public boolean subsumes(Object general, Object specific)
	{
		return false;
	}	
}