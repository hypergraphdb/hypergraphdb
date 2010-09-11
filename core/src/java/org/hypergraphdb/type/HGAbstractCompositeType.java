/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Iterator;
import java.util.HashMap;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;

/**
 * 
 * <p>
 * Represents an abstract type (can't be instantiated) that has some properties - modeled
 * after and mainly used to represent an abstract Java bean.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGAbstractCompositeType extends HGAbstractType implements HGCompositeType 
{
	public static class Projection implements HGProjection
	{
		private String name;
		private HGHandle type;
		
		public Projection(String name, HGHandle type)
		{
			this.name = name;
			this.type = type;
		}
		
		public int[] getLayoutPath() 
		{
			return null;
		}

		public String getName() 
		{
			return name;
		}

		public HGHandle getType() 
		{
			return type;
		}

		public void inject(Object atomValue, Object value) 
		{
			throw new HGException("Cannot inject value using an abstract type.");
		}

		public Object project(Object atomValue) 
		{
			throw new HGException("Cannot project value using an abstract type.");			
		}
		
	}
	
	private HashMap<String, Projection> projections = new HashMap<String, Projection>();
	
	public void addProjection(Projection p)
	{
		projections.put(p.getName(), p);
	}
	
	public void removeProjection(String name)
	{
		projections.remove(name);
	}
	
	public Iterator<String> getDimensionNames() 
	{
		return projections.keySet().iterator();
	}

	public HGProjection getProjection(String dimensionName) 
	{
		return projections.get(dimensionName);
	}
}