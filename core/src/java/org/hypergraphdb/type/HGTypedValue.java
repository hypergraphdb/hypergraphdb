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
 * <p>
 * This is a utility class to manipulate arbitrary HyperGraph typed
 * objects, that are not necessarily stored as HyperGraph atoms. It simply
 * holds a reference to a <code>java.lang.Object</code> instance and its
 * <code>HGAtomType</code> type instance.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGTypedValue 
{
	private Object value;
	private HGHandle type;
	
	public HGTypedValue(Object value, HGHandle type)
	{
		this.value = value;
		this.type = type;
	}
	
	public Object getValue()
	{
		return value;
	}
	
	public HGHandle getType()
	{
		return type;
	}
}
