/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.algorithms;

import java.util.Iterator;

import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
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
	private HyperGraph hg;
	private TempLink tempLink = new TempLink(HyperGraph.EMTPY_HANDLE_SET);	
	private HGHandle hCurrLink;
	private AdjIterator currIterator = null;
	
	private class AdjIterator implements HGSearchResult<HGHandle>
	{
		HGHandle src;
		Iterator<HGHandle> linksIterator;
		HGLink currLink;
		HGHandle current;
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
				if (hg.isLoaded(hCurrLink))
					currLink = (HGLink)hg.get(hCurrLink);
				else
				{
					tempLink.setHandleArray(hg.getStore().getLink(hg.getPersistentHandle(hCurrLink)), 2);
					currLink = tempLink;
				}
			}
			if (currLink.getTargetAt(0).equals(src))
				currLinkPos = 1;
			else
				currLinkPos = 0;
		}
				
		AdjIterator(HGHandle src, Iterator<HGHandle> linksIterator, boolean closeResultSet)
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
		
		public HGHandle next()
		{
			current = currLink.getTargetAt(currLinkPos);
			
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

		public HGHandle current()
		{
			return current;
		}

		public boolean isOrdered()
		{
			return false;
		}

		public boolean hasPrev() { throw new UnsupportedOperationException(); }
		public HGHandle prev() { throw new UnsupportedOperationException(); }				
	}
	
	/**
	 * <p>Construct a <code>SimpleALGenerator</code> for the given HyperGraph instance.</p>
	 * 
	 * @param hg The HyperGraph instance.
	 */
	public SimpleALGenerator(HyperGraph hg)
	{
		this.hg = hg;
	}
	
	public HGHandle getCurrentLink()
	{
		return hCurrLink;
	}
	
	public HGSearchResult<HGHandle> generate(HGHandle h) 
	{
		if (hg.isIncidenceSetLoaded(h))
			return new AdjIterator(
					h, 
					hg.getIncidenceSet(h).iterator(), 
					false);			
		else
			return new AdjIterator(
					h, 
					hg.getIncidenceSet(h).getSearchResult(), 
					true);
	}
	
	public void close()
	{
		if (currIterator != null && currIterator.closeResultSet)
			((HGSearchResult<HGHandle>)currIterator.linksIterator).close();
	}
}