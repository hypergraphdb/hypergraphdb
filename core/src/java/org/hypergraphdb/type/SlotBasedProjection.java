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
 * </p>
 * @author Borislav Iordanov
 */
class SlotBasedProjection implements HGProjection 
{
	private Slot slot;
	private int [] layoutPath;
	
	public SlotBasedProjection(Slot slot, int [] layoutPath)
	{
		this.slot = slot;
		this.layoutPath = layoutPath;
	}
	
	public String getName() 
	{
		return slot.getLabel();
	}

	public HGHandle getType() 
	{
		return slot.getValueType();
	}

	public int[] getLayoutPath() 
	{
		return layoutPath;
	}

	public Object project(Object value) 
	{
		return ((Record)value).get(slot);
	}
	
	public void inject(Object record, Object value)
	{
		((Record)record).set(slot, value);
	}
}
