/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.algorithms;

import java.util.LinkedList;
import java.util.Queue;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Implements a depth-first search of a graph. As a reminder, breadth-first will visit all atoms 
 * in an adjency list before exploring their adjacent atoms in turn.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGBreadthFirstTraversal implements HGTraversal 
{
	private HGHandle startAtom;
	private int maxDistance; // the maximum reachable distance from the starting node
	private HGAtomSet visited = new HGAtomSet();
	private Queue<Pair<Pair<HGHandle, HGHandle>, Integer>> to_explore = 
	    new LinkedList<Pair<Pair<HGHandle, HGHandle>, Integer>>();
	private HGALGenerator adjListGenerator;
	private boolean initialized = false;
	
	private void init()
	{
        visited.add(startAtom);
        advance(startAtom, 0);     	    
        initialized = true;
	}
	
	private void advance(HGHandle from, int distance)
	{
	    if (distance >= maxDistance)
	        return;
	    
		HGSearchResult<HGHandle> i = adjListGenerator.generate(from);
		Integer dd = distance + 1;
		while (i.hasNext())
		{
			HGHandle link = adjListGenerator.getCurrentLink();
			HGHandle h = i.next();
			if (!visited.contains(h))
			{
			    Pair<HGHandle, HGHandle> p = new Pair<HGHandle, HGHandle>(link, h);
				to_explore.add(new Pair<Pair<HGHandle, HGHandle>, Integer>(p, dd));
			}
		}
		i.close();
	}
	
	public void setStartAtom(HGHandle startAtom)
	{
		this.startAtom = startAtom;
	}
	
	public HGHandle getStartAtom()
	{
		return startAtom;
	}
	
	public HGALGenerator getAdjListGenerator()
	{
		return adjListGenerator;
	}

	public void setAdjListGenerator(HGALGenerator adjListGenerator)
	{
		this.adjListGenerator = adjListGenerator;
	}
	
	public void remove() 
	{
		throw new UnsupportedOperationException();
	}

	public HGBreadthFirstTraversal()
	{		
	}
	
	public HGBreadthFirstTraversal(HGHandle startAtom, HGALGenerator adjListGenerator)	
	{
	    this(startAtom, adjListGenerator, Integer.MAX_VALUE);
	}
	
	public HGBreadthFirstTraversal(HGHandle startAtom, HGALGenerator adjListGenerator, int maxDistance)
	{
	    this.maxDistance = maxDistance;
        this.startAtom = startAtom;
        this.adjListGenerator = adjListGenerator;
        init();
	}
	public boolean hasNext() 
	{
		if (!initialized)
			init();
		return !to_explore.isEmpty();
	}

	public boolean isVisited(HGHandle handle) 
	{
		return visited.contains(handle);
	}

	public Pair<HGHandle, HGHandle> next() 
	{
		if (!initialized)
			init();		
	    Pair<HGHandle, HGHandle> rvalue = null;		
		if (!to_explore.isEmpty())
		{
		    Pair<Pair<HGHandle, HGHandle>, Integer> x = to_explore.remove();
			rvalue = x.getFirst();
			visited.add(rvalue.getSecond());
			advance(rvalue.getSecond(), x.getSecond());
		}
		return rvalue;
	}
	
	public void reset()
	{
		visited.clear();
		to_explore.clear();
		init();
	}
}