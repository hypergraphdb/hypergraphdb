/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * Implements a depth-first search of a graph. As a reminder, depth-first will visit atoms adjacent
 * to the current before visiting its siblings.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGDepthFirstTraversal implements HGTraversal 
{
	private Ref<HGHandle> startAtom;
    // The following maps contains all atoms that have been reached: if they have
    // been actually visited (i.e. returned by the 'next' method), they map to 
    // Boolean.TRUE, otherwise they map to Boolean.FALSE.
    private Map<HGHandle, Boolean> examined = new HashMap<HGHandle, Boolean>();
	private Stack<Pair<HGHandle, HGHandle>> to_explore = new Stack<Pair<HGHandle, HGHandle>>();
	private HGALGenerator adjListGenerator;
	private volatile boolean initialized = false;
	
	private void init()
	{
	    examined.put(startAtom.get(), Boolean.TRUE);
        advance(startAtom.get());     	    
        initialized = true;
	}
	
	private void advance(HGHandle from)
	{
		HGSearchResult<Pair<HGHandle, HGHandle>> i = adjListGenerator.generate(from);
		while (i.hasNext())
		{
		    Pair<HGHandle, HGHandle> p = i.next();
			if (!examined.containsKey(p.getSecond()))
			{
				to_explore.push(p);
				examined.put(p.getSecond(), Boolean.FALSE);
			}
		}
		i.close();
	}
	
	public HGDepthFirstTraversal()
	{		
	}
	
	public HGDepthFirstTraversal(HGHandle startAtom, HGALGenerator adjListGenerator)
	{
		this(hg.constant(startAtom), adjListGenerator);
	}

	public HGDepthFirstTraversal(Ref<HGHandle> startAtom, HGALGenerator adjListGenerator)
	{
		this.startAtom = startAtom;
		this.adjListGenerator = adjListGenerator;
		
        // we need to lazily initialize because of compiled queries where the start atom handle
        // is not necessarily known at construction time
		
//		init();		
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

	public boolean hasNext() 
	{
		if (!initialized)
			init();
		return !to_explore.isEmpty();
	}

	public Pair<HGHandle, HGHandle> next() 
	{
		if (!initialized)
			init();
		Pair<HGHandle, HGHandle> rvalue = null;
		if (!to_explore.isEmpty())
		{
			rvalue = to_explore.pop();
			examined.put(rvalue.getSecond(), Boolean.TRUE);
			advance(rvalue.getSecond());
		}
		return rvalue;
	}

	public boolean isVisited(HGHandle handle) 
	{
        Boolean b = examined.get(handle);
        return b != null && b;
	}

	public void remove() 
	{
		throw new UnsupportedOperationException();
	}
	
	public void reset()
	{
		examined.clear();
		to_explore.clear();
		init();
	}	
}
