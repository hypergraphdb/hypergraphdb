/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

public class HGAtomReplacedEvent extends HGAtomEvent 
{
	public HGAtomReplacedEvent(HGHandle handle)
	{
		super(handle);
	}
}
