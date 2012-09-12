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
 * This is triggered after a new atom, node or link, has been added to
 * to HyperGraph. The result returned by event handlers is ignored by
 * <code>HyperGraph</code>. 
 * </p>
 */
public class HGAtomAddedEvent extends HGAtomEvent 
{    
	public HGAtomAddedEvent(HGHandle handle, Object source)
	{
		super(handle, source);
	}
}