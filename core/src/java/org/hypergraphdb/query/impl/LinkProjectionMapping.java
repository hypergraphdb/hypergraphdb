/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.util.Mapping;

public final class LinkProjectionMapping implements Mapping<HGLink, HGHandle>
{
	private int targetPosition;
	
	public LinkProjectionMapping()
	{
		
	}
	public LinkProjectionMapping(int targetPosition)
	{
		this.targetPosition = targetPosition;
	}
	
	public HGHandle eval(HGLink x)
	{
		return x.getTargetAt(targetPosition);
	}
	public int getTargetPosition()
	{
		return targetPosition;
	}
	public void setTargetPosition(int targetPosition)
	{
		this.targetPosition = targetPosition;
	}
	
	
}
