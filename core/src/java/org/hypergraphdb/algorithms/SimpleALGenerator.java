/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.Iterator;

import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.TempLink;

/**
 * <p>
 * The <code>SimpleALGenerator</code> produces all atoms linked to the given atom,
 * regardless of the link type and regardless of how an outgoing set is ordered.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class SimpleALGenerator implements HGALGenerator 
{
	protected HyperGraph graph;
	private TempLink tempLink = new TempLink(HyperGraph.EMPTY_HANDLE_SET);	
	private AdjIterator currIterator = null;
	
	protected class AdjIterator implements HGSearchResult<Pair<HGHandle,HGHandle>>
	{
		HGHandle src;
		Iterator<HGHandle> linksIterator;
		HGHandle hCurrLink;
		HGLink currLink;
		Pair<HGHandle,HGHandle> current;
		int currLinkPos;
		boolean closeResultSet;
		
		private void getNextLink()
		{
			// loop makes sure that we skip links that only point to our 'src' atom and nothing else
			for (boolean done = false; !done; done = currLink.getArity() > 1)
			{
				if (!linksIterator.hasNext())
				{
					currLink = null;
					if (closeResultSet)
						((HGSearchResult<HGHandle>)linksIterator).close();
					return;
				}
				hCurrLink = linksIterator.next(); 
				if (graph.isLoaded(hCurrLink))
					currLink = (HGLink)graph.get(hCurrLink);
				else
				{
					tempLink.setHandleArray(graph.getStore().getLink(graph.getPersistentHandle(hCurrLink)), 2);
					currLink = tempLink;
				}
			}
			if (currLink.getTargetAt(0).equals(src))
				currLinkPos = 1;
			else
				currLinkPos = 0;
		}
				
		public AdjIterator(HGHandle src, Iterator<HGHandle> linksIterator, boolean closeResultSet)
		{
			this.src = src;
			this.linksIterator = linksIterator;
			this.closeResultSet = closeResultSet;
			getNextLink();
		}
		
		public void remove() { throw new UnsupportedOperationException(); }
		
		public boolean hasNext()
		{
			return currLink != null;
		}
		
		public Pair<HGHandle,HGHandle> next()
		{
			current = new Pair<HGHandle,HGHandle>(hCurrLink, currLink.getTargetAt(currLinkPos));	        
			// advance within link, then check whether we're pointing to 'src' and, if so, advance again
			if (++currLinkPos == currLink.getArity())
				getNextLink();
			else if (currLink.getTargetAt(currLinkPos).equals(src))
				if (++currLinkPos == currLink.getArity())
					getNextLink();			
			return current;
		}

		public void close()
		{
			if (closeResultSet)
				((HGSearchResult<HGHandle>)linksIterator).close();			
		}

		public Pair<HGHandle,HGHandle> current()
		{
			return current;
		}

		public boolean isOrdered()
		{
			return false;
		}

		public boolean hasPrev() { throw new UnsupportedOperationException(); }
		public Pair<HGHandle,HGHandle> prev() { throw new UnsupportedOperationException(); }				
	}

	/**
	 * <p>
	 * Empty constructor - you will need to set the graph (see {@link setGraph}) before
	 * the instance becomes usable.
	 * </p>
	 */
	public SimpleALGenerator()
	{		
	}
	
	/**
	 * <p>Construct a <code>SimpleALGenerator</code> for the given HyperGraph instance.</p>
	 * 
	 * @param hg The HyperGraph instance.
	 */
	public SimpleALGenerator(HyperGraph hg)
	{
		this.graph = hg;
	}
	
	public HGSearchResult<Pair<HGHandle,HGHandle>> generate(HGHandle h) 
	{
		return new AdjIterator(
				h, 
				graph.getIncidenceSet(h).getSearchResult(), 
				true);
	}
	
	public void close()
	{
		if (currIterator != null && currIterator.closeResultSet)
			((HGSearchResult<HGHandle>)currIterator.linksIterator).close();
	}
	
	public void setGraph(HyperGraph graph)
	{
		this.graph = graph;
	}
	
	public HyperGraph getGraph()
	{
		return this.graph;
	}
}