/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * Implements a breadth-first search of a graph. As a reminder, breadth-first will visit all atoms 
 * in an adjacency list before exploring their adjacent atoms in turn.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGBreadthFirstTraversal implements HGTraversal 
{
	private Ref<HGHandle> startAtom;
	private int maxDistance = Integer.MAX_VALUE; // the maximum reachable distance from the starting node
	// The following maps contains all atoms that have been reached: if they have
	// been actually visited (i.e. returned by the 'next' method), they map to 
	// Boolean.TRUE, otherwise they map to Boolean.FALSE.
	private Map<HGHandle, Boolean> examined = new HashMap<HGHandle, Boolean>();
	private Queue<Pair<Pair<HGHandle, HGHandle>, Integer>> to_explore = 
	    new LinkedList<Pair<Pair<HGHandle, HGHandle>, Integer>>();
	private HGALGenerator adjListGenerator;
	private boolean initialized = false;
	
	private void init()
	{
        examined.put(startAtom.get(), Boolean.TRUE);
        advance(startAtom.get(), 0);     	    
        initialized = true;        
	}
	
	private void advance(HGHandle from, int distance)
	{
	    if (distance >= maxDistance)
	        return;
	    
		HGSearchResult<Pair<HGHandle, HGHandle>> i = adjListGenerator.generate(from);
		Integer dd = distance + 1;
		while (i.hasNext())
		{
		    Pair<HGHandle, HGHandle> p = i.next();
			if (!examined.containsKey(p.getSecond()))
			{
				to_explore.add(new Pair<Pair<HGHandle, HGHandle>, Integer>(p, dd));
				examined.put(p.getSecond(), Boolean.FALSE);
			}
		}
		i.close();
	}
	
	public Ref<HGHandle> getStartAtomReference()
	{
		return startAtom;
	}
	
	public void setStartAtomReference(Ref<HGHandle> startAtom)
	{
		this.startAtom = startAtom;
	}
	
	public void setStartAtom(HGHandle startAtom)
	{
		this.startAtom = hg.constant(startAtom);
	}
	
	public HGHandle getStartAtom()
	{
		return startAtom == null ? null : startAtom.get();
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
	
	public HGBreadthFirstTraversal(Ref<HGHandle> startAtom, HGALGenerator adjListGenerator)
	{
		this(startAtom, adjListGenerator, Integer.MAX_VALUE);
	}
	
	public HGBreadthFirstTraversal(HGHandle startAtom, HGALGenerator adjListGenerator, int maxDistance)
	{
		this(hg.constant(startAtom), adjListGenerator, maxDistance);
	}
	
	public HGBreadthFirstTraversal(Ref<HGHandle> startAtom, HGALGenerator adjListGenerator, int maxDistance)
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
	    Boolean b = examined.get(handle);
		return b != null && b;
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
			examined.put(rvalue.getSecond(), Boolean.TRUE);
			advance(rvalue.getSecond(), x.getSecond());
		}
		return rvalue;
	}
	
	public void reset()
	{
		examined.clear();
		to_explore.clear();
		init();
	}
}
