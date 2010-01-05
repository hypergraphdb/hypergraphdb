/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;

/**
 * @author Borislav Iordanov
 */
public class BeanPropertyBasedProjection implements HGProjection 
{
	private HGProjection wrapped;
	
	public BeanPropertyBasedProjection(HGProjection wrapped)
	{
		this.wrapped = wrapped;
	}
	
	public String getName() 
	{
		return wrapped.getName();
	}

	public HGHandle getType() 
	{
		return wrapped.getType();
	}

	public int[] getLayoutPath() 
	{
		return wrapped.getLayoutPath();
	}

	public Object project(Object value) 
	{
		return BonesOfBeans.getProperty(value, getName());
	}
	
	public void inject(Object bean, Object value)
	{
		BonesOfBeans.setProperty(bean, getName(), value);
	}
}
