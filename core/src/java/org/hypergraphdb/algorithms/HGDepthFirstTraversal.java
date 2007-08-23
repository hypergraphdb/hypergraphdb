/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.algorithms;

import java.util.Stack;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.util.Pair;

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
	private HGHandle startAtom;
	private HGAtomSet visited = new HGAtomSet();
	private Stack<Pair<HGHandle, HGHandle>> to_explore = new Stack<Pair<HGHandle, HGHandle>>();
	private HGALGenerator adjListGenerator;
	
	private void advance(HGHandle from)
	{
		HGSearchResult<HGHandle> i = adjListGenerator.generate(from);
		while (i.hasNext())
		{
			HGHandle link = adjListGenerator.getCurrentLink();
			HGHandle h = i.next();
			if (!visited.contains(h))
				to_explore.push(new Pair<HGHandle, HGHandle>(link, h));
		}
		i.close();
	}
	
	public HGDepthFirstTraversal(HGHandle startAtom, HGALGenerator adjListGenerator)
	{
		this.startAtom = startAtom;
		this.adjListGenerator = adjListGenerator;
		visited.add(startAtom);
		advance(startAtom);
	}

	public HGHandle getStartAtom()
	{
		return startAtom;
	}
	
	public boolean hasNext() 
	{
		return !to_explore.isEmpty();
	}

	public Pair<HGHandle, HGHandle> next() 
	{
		Pair<HGHandle, HGHandle> rvalue = null;
		if (!to_explore.isEmpty())
		{
			rvalue = to_explore.pop();
			visited.add(rvalue.getSecond());
			advance(rvalue.getSecond());
		}
		return rvalue;
	}

	public boolean isVisited(HGHandle handle) 
	{
		return visited.contains(handle);
	}

	public void remove() 
	{
		throw new UnsupportedOperationException();
	}
}