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
 * An <code>HGAtomEvictedEvent</code> is triggered by the cache when an atom's run-time
 * instance is removed from memory.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGAtomEvictEvent extends HGAtomEvent 
{
	private Object instance;
	
	public HGAtomEvictEvent(HGHandle handle, Object instance)
	{
		super(handle);
		this.instance = instance;
	}
	
	public Object getInstance()
	{
		return instance;
	}
}
