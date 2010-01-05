/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

public class TraversalBasedQuery extends HGQuery 
{
	private HGTraversal traversal;
	private ReturnType returnType = ReturnType.both;
	
	public static enum ReturnType
	{
		targets,
		links,
		both
	};
	
	public TraversalBasedQuery(HGTraversal traversal)
	{
		this.traversal = traversal;
	}
	
	public TraversalBasedQuery(HGTraversal traversal, ReturnType returnType)
	{
		this(traversal);
		this.returnType = returnType;
	}
	
	public HGTraversal getTraversal()
	{
		return traversal;
	}
	
	@Override
	public HGSearchResult<?> execute() 
	{
		switch (returnType)
		{
			case both: 
				return new TraversalResult(traversal);
			case targets:
				return new MappedResult(new TraversalResult(traversal), 
						new Mapping<Pair<HGHandle, HGHandle>, HGHandle>()
				{
					public HGHandle eval(Pair<HGHandle, HGHandle>  p) { return p.getSecond(); }
				}
				);
			case links:
				return new MappedResult(new TraversalResult(traversal), 
						new Mapping<Pair<HGHandle, HGHandle>, HGHandle>()
				{
					public HGHandle eval(Pair<HGHandle, HGHandle>  p) { return p.getFirst(); }
				}
				);
			default:
				throw new HGException("This should never happen ;)");
		}
	}

	public ReturnType getReturnType()
	{
		return returnType;
	}

	public void setReturnType(ReturnType returnType)
	{
		this.returnType = returnType;
	}	
}
