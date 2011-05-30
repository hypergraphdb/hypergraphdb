/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

public class MapCondition implements HGQueryCondition
{
	private HGQueryCondition cond;
	private Mapping<?, ?> mapping;
	
	public MapCondition()
	{
		
	}
	
	public MapCondition(HGQueryCondition condition, Mapping<?, ?> mapping)
	{
		this.cond = condition;
		this.mapping = mapping;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle)
	{
		throw new UnsupportedOperationException();
	}

	public HGQueryCondition getCondition()
	{
		return cond;
	}

	public void setCondition(HGQueryCondition cond)
	{
		this.cond = cond;
	}

	public Mapping<?, ?> getMapping()
	{
		return mapping;
	}
	
	public void setMapping(Mapping<?, ?> mapping)
	{
		this.mapping = mapping;
	}

	public int hashCode() 
	{ 
		return HGUtils.hashThem(cond, mapping);  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof MapCondition))
			return false;		
		else
		{
			MapCondition c = (MapCondition)x;
			return HGUtils.eq(cond, c.cond) &&
				   HGUtils.eq(mapping, c.mapping);
		}
	}
	
	public String toString()
	{
	    return "map(" + this.getMapping() + "," + this.getCondition() + ")";
	}
}
