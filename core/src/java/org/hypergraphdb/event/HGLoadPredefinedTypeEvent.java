/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGPersistentHandle;

/**
 * <p>
 * This event is triggered by the type system when the run-time instance of a 
 * predefined type needs to be loaded in the cache. Applications should listen
 * to this event whenever predefined need to be loaded on demand and HyperGraph
 * cannot perform this task alone. 
 * </p>
 * 
 * <p>
 * A listener is expected to add the predefined refered by the <code>typeHandle</code>
 * attribute of the event instance. Predefined types are added through a call to the
 * <code>HGTypeSystem.addPredefinedType</code> method.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGLoadPredefinedTypeEvent extends HGEventBase 
{
	private HGPersistentHandle typeHandle;
	
	public HGLoadPredefinedTypeEvent(HGPersistentHandle typeHandle)
	{
		this.typeHandle = typeHandle;		 
	}
	
	public HGPersistentHandle getTypeHandle()
	{
		return typeHandle;
	}
}
