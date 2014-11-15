/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import java.util.HashMap;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGProjection;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * An <code>AtomProjectionCondition</code> will yield all atoms that are
 * projections along a certain dimension of a given base atom set. The
 * base atom set is specified as a <code>HGQueryCondition</code>. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomProjectionCondition implements HGQueryCondition, HGAtomPredicate 
{
	private String [] dimensionPath;
	private HGQueryCondition baseSetCondition;
	private HashMap<HGHandle, HGHandle> baseSet = null;
	
	public AtomProjectionCondition()
	{
		
	}
	
	public AtomProjectionCondition(String dimensionPath, HGQueryCondition baseSetCondition)
	{
		this.dimensionPath = TypeUtils.parseDimensionPath(dimensionPath);
		this.baseSetCondition = baseSetCondition;
		 
	}
	
	public AtomProjectionCondition(String [] dimensionPath, HGQueryCondition baseSetCondition)
	{
		this.dimensionPath = dimensionPath;
		this.baseSetCondition = baseSetCondition;		 
	}
	
	public String [] getDimensionPath()
	{
		return this.dimensionPath;
	}
	
	public HGQueryCondition getBaseSetCondition()
	{
		return this.baseSetCondition;
	}
	
	public boolean satisfies(HyperGraph graph, HGHandle handle) 
	{
		if (baseSet == null)
		{
			baseSet = new HashMap<HGHandle, HGHandle>();
			HGSearchResult<HGHandle> rs = null;
			try
			{
				rs = graph.find(baseSetCondition);
				while (rs.hasNext())
				{
					HGHandle h = rs.next();
					HGAtomType ot = graph.getTypeSystem().getAtomType(h);
					HGProjection proj = TypeUtils.getProjection(graph, ot, dimensionPath);
					if (proj != null)					
					{
						Object part = proj.project(graph.get(h));
						if (part != null)
							baseSet.put(graph.getHandle(part), h);
					}
				}
			}
			finally
			{
				HGUtils.closeNoException(rs);
			}
		}		
		return baseSet.containsKey(handle);
	}
		
	public int hashCode() 
	{ 
		return HGUtils.hashThem(dimensionPath, baseSetCondition); 
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomProjectionCondition))
			return false;
		else
		{
			AtomProjectionCondition c = (AtomProjectionCondition)x;
			return HGUtils.eq(dimensionPath, c.dimensionPath) &&
				   HGUtils.eq(baseSetCondition, c.baseSetCondition);
		}
	}

	public void setDimensionPath(String[] dimensionPath)
	{
		this.dimensionPath = dimensionPath;
	}

	public void setBaseSetCondition(HGQueryCondition baseSetCondition)
	{
		this.baseSetCondition = baseSetCondition;
	}
	
	
	
}
