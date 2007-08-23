package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGLink;
import org.hypergraphdb.util.Mapping;

public class LinkProjectionMapping implements Mapping
{
	private int targetPosition;
	
	public LinkProjectionMapping(int targetPosition)
	{
		this.targetPosition = targetPosition;
	}
	
	public Object eval(Object x)
	{
		return ((HGLink)x).getTargetAt(targetPosition);
	}

}
