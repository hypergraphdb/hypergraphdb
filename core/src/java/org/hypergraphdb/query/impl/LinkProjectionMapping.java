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
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Ref;

public final class LinkProjectionMapping implements Mapping<HGLink, HGHandle>
{
	private Ref<Integer> targetPosition;
	
	public LinkProjectionMapping()
	{
		
	}
	public LinkProjectionMapping(int targetPosition)
	{
		this.targetPosition = hg.constant(targetPosition);
	}
	public LinkProjectionMapping(Ref<Integer> targetPosition)
	{
		this.targetPosition = targetPosition;
	}
	
	public HGHandle eval(HGLink x)
	{
		return x.getTargetAt(targetPosition.get());
	}
	public int getTargetPosition()
	{
		return targetPosition.get();
	}
	public void setTargetPosition(int targetPosition)
	{
		this.targetPosition = hg.constant(targetPosition);
	}
	public Ref<Integer> getTargetPositionReference()
	{
		return this.targetPosition;
	}
	public void setTargetPositionReference(Ref<Integer> tp)
	{
		this.targetPosition = tp;
	}
}