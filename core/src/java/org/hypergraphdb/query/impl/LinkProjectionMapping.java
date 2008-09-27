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