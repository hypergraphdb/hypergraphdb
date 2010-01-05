/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.lang.reflect.*;
import java.util.Collections;
import java.util.Map;

import org.hypergraphdb.HGException;

/**
 * 
 * <p>
 * Utility class to maintain runtime caches of Field, Method and Constructor for
 * use by <code>HGAtomType</code> implementations.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class AccessibleObjectCache
{
	private Map<Pair<Class<?>, String>, Field> fields = 
		Collections.synchronizedMap(new SoftHashMap<Pair<Class<?>, String>, Field>());
	
	private Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException
	{
		Field result = null;
		while (clazz != null && result == null)
		{
			Field [] all = clazz.getDeclaredFields();
			for (int i = 0; i < all.length; i++)
				if (all[i].getName().equals(fieldName))
				{
					result = all[i];
					result.setAccessible(true);
					break;
				}
			 clazz = clazz.getSuperclass();			
		}
		if (result == null) 
			throw new NoSuchFieldException("Class " + clazz.getName() + ", field " + fieldName);
		return result;
	}
	
	public Field getField(Class<?> clazz, String fieldName)
	{
		Pair<Class<?>, String> key = new Pair<Class<?>, String>(clazz, fieldName);
		Field f = fields.get(key);
		if (f == null)
		{
			try
			{
				f = findField(clazz, fieldName);
			}
			catch (Exception ex)
			{
				throw new HGException(ex);
			}		
			fields.put(key, f);
		}
		return f;
	}
}
