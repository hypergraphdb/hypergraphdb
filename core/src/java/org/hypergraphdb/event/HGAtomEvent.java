/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * This is a base class for various atom related events. 
 * </p>
 */
public abstract class HGAtomEvent implements HGEvent 
{
	private HGHandle handle;
	
	public HGAtomEvent(HGHandle handle)
	{
		this.handle = handle;
	}
	
	public HGHandle getAtomHandle()
	{
		return handle;
	}
}