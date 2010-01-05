/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.type.TypeUtils;

public class ProjectionAtomResultSet implements HGSearchResult 
{
    private HyperGraph graph;
	private HGSearchResult searchResult;
	private HGHandle baseType = null;
	private String [] dimensionPath;		
	private Object current = null;	
	
	//
	// The number of elements preceeding the current in the underlying
	// searchResult that satisfy the predicate.
	//
	private int prevCount = 0;
	
	//
	// The diff in the underlying result b/w the location of our 'current' member
	// variable and its own "current" element. 
	// 
	private int lookahead = 0; 

	private Object examine(HGHandle h)
	{
		HGHandle ot = baseType == null ? graph.getTypeSystem().getTypeHandle(h) : baseType;
		HGTypedValue tv = TypeUtils.project(graph, ot, graph.get(h), dimensionPath, false);
		return tv != null ? graph.getHandle(tv.getValue()) : null;
	}
	
	/**
	 * <p>
	 * The constructor assumes the underlying set is already positioned to the
	 * first matching entity.
	 * </p>
	 * 
	 * @param graph The 
	 * @param searchResult
	 * @param predicate
	 */
	public ProjectionAtomResultSet(HyperGraph graph, 
							 	   HGSearchResult searchResult,
							 	   String [] dimensionPath,
							 	   HGHandle baseType)
	{
        this.graph = graph;
		this.searchResult = searchResult;
		this.dimensionPath = dimensionPath;
		this.baseType = baseType;
	}
	
	public void close() 
	{
		searchResult.close();
	}

	public Object current() 
	{
		return current;
	}
	
	public boolean hasPrev() 
	{
		return prevCount > 0;
	}

	public Object prev() 
	{
		if (prevCount == 0)
			throw new NoSuchElementException();
		while (lookahead > 0)
		{
			lookahead--;
			searchResult.prev();
		}
		prevCount--;			
		while (searchResult.hasPrev() && examine((HGHandle)searchResult.prev()) == null);
		return current = examine((HGHandle)searchResult.current());
	}

	public boolean hasNext() 
	{
		if (lookahead > 0)
			return examine((HGHandle)searchResult.current()) != null;
		
		while (true)
		{
			if (!searchResult.hasNext())
				return false;
			lookahead++;
			if (examine((HGHandle)searchResult.next()) == null)
				continue;
			else				
				return true;
		} 			
	}

	public Object next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		else
		{
			prevCount++;
			lookahead = 0;
			return current = examine((HGHandle)searchResult.current());
		}
	}
	
	public void remove() 
	{
		throw new UnsupportedOperationException();
	}
	
	public boolean isOrdered()
	{
		return searchResult.isOrdered();
	}

}
