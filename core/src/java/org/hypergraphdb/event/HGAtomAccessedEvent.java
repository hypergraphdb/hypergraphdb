/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p> 
 * This event is triggered every time an atom is accessed by its handle in HyperGraph.
 * </p>
 */
public class HGAtomAccessedEvent extends HGAtomEvent 
{
	private Object instance;
	
	public HGAtomAccessedEvent(HGHandle handle, Object instance)
	{
		super(handle);
		this.instance = instance;
	}
	
	public Object getAtomInstance()
	{
		return instance;
	}
}
