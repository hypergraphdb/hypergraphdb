/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.Iterator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.CloseMe;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.TempLink;

/**
 * <p>
 * A default implementation of the <code>HGALGenerator</code> that should cover most
 * common cases. In the description below, the term <em>focus atom</em> is used to refer
 * to the atom whose adjacency list is being generated. 
 * </p>
 * 
 * <p>
 * The adjacency list generation process is conceptually split into two main steps:
 * 
 * <ol>
 * <li>Get the relevant links for the atom.</li>
 * <li>For each link, get the relevant members of its outgoing set.</li>
 * </ol>
 * 
 * In the simplest case, step 1 amounts to retrieving the incidence set of the focus atom
 * and considering only links that point to other atoms besides it (i.e. links with
 * arity > 1), while step 2 amounts to retrieving all atoms from the currently considered 
 * link that are different from the focus atom.  
 * </p>
 * 
 * <p>
 * Step 1 may be augmented with a filter to select links only satisfying certain criteria.
 * This filter is configured in the form of a link predicate, a <code>HGQueryCondition</code>.
 * </p>
 * 
 * <p>
 * Step 2 may be configured to treat links as ordered. When links are interpreted as ordered, there
 * are several further options:
 * 
 * <ul>
 * <li>The ordering of the outgoing set may be the one implied by the link instance or its reverse.</li>
 * <li>Given the position of the focus atom within a link's outgoing set, the generator may
 * return only the siblings that occur <strong>after</strong> that position or <strong>before</strong>
 * or <strong>both</strong>. Choosing to return both only makes sense when the outgoing set is 
 * processed in reverse order, for otherwise the behavior is the same as if a link is treated 
 * as unordered.</li>
 * </ul>
 * 
 * In addition, step 2 may also filter the sibling atoms by a general predicate similarly to 
 * the way links from the incidence set are filtered.
 * </p>
 * 
 * <p>
 * All of the above mentioned options are configured at construction time. In the simplest case
 * of no link or sibling filtering and where links are unordered, use the <code>SimpleALGenerator</code>
 * instead which will be somewhat faster.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class DefaultALGenerator implements HGALGenerator, CloseMe
{
	protected HyperGraph hg;
	private TempLink tempLink = new TempLink(HyperGraph.EMPTY_HANDLE_SET);
	private HGAtomPredicate linkPredicate;
	private HGAtomPredicate siblingPredicate;
	private boolean returnPreceeding = true, 
				    returnSucceeding = true, 
				    reverseOrder = false,
				    returnSource = false;
	private AdjIterator currIterator = null;
	
	protected class AdjIterator implements HGSearchResult<Pair<HGHandle, HGHandle>>
	{
		HGHandle src;
		Iterator<HGHandle> linksIterator;
		HGLink currLink;
	    HGHandle hCurrLink;		
	    Pair<HGHandle, HGHandle> current;
		TargetSetIterator tsIter;
		boolean closeResultSet;
		int minArity = 2;
		
		//
		// TargetSetIterator is used to iterate within the target set of a given link.
		// It takes care of the 'siblingPredicate'. There are two version of it:
		// FTargetSetIterator (starting from 0 and going forward in the target set) and
		// BTargetSetIterator (starting from arity-1 and going backward in the target set).
		// The two version all almost identical, except that the first increments the 'pos'
		// index into the target set while the second decrements it.
		//
		private abstract class TargetSetIterator 
		{
			boolean focus_seen = false;
			int pos = 0;
			
			abstract void reset();
			abstract void advance();
			boolean hasNext() { return pos != -1; }
			
			public HGHandle next()
			{
				HGHandle rvalue = currLink.getTargetAt(pos);
				advance();
				return rvalue;
			}			
		}
		
		final class FTargetSetIterator extends TargetSetIterator
		{
			void filter()
			{
				while (true)
				{
					HGHandle h = currLink.getTargetAt(pos);
					if (!focus_seen && h.equals(src))
					{
						focus_seen = true;
						if (returnSource && siblingPredicate.satisfies(hg, h))
							return;						
						else if (!returnSucceeding)
						{
							pos = -1;
							return;
						}						
					}
					else if (siblingPredicate.satisfies(hg, h))
						return;
					if (++pos == currLink.getArity())
					{
						pos = -1;
						return;
					}
				}				
			}
			
			void advance()
			{
				if (++pos == currLink.getArity())
				{
					pos = -1;
					return;
				}
				else if (siblingPredicate != null)
					filter();
				else if (!focus_seen && currLink.getTargetAt(pos).equals(src))
				{
					focus_seen = true;
					if (returnSource)
						return;
					else if (!returnSucceeding || ++pos == currLink.getArity())
						pos = -1;
				}
			}
			
			void reset()
			{
				pos = 0;
				focus_seen = false;
				if (!returnPreceeding)
				{
					while (!currLink.getTargetAt(pos++).equals(src));
					focus_seen = true;
					if (returnSource && 
						(siblingPredicate == null || siblingPredicate.satisfies(hg, src)))
					{
						pos--;
						return;
					}
					else if (pos == currLink.getArity())
					{
						pos = -1;
						return;
					}
				}
				if (siblingPredicate != null)
					filter();		
				else if (!focus_seen && currLink.getTargetAt(pos).equals(src))
				{
					focus_seen = true;
					if (returnSource && (siblingPredicate == null || siblingPredicate.satisfies(hg, src)))
						return;
					else if (!returnSucceeding)
					{
						pos = -1;
						return;
					}
					else
						pos++;
				}
			}
		}
			
		// this is almost identical to FTargetSetIterator above, except we decrement the 'pos'
		// cursor instead of incrementing it.  
		final class BTargetSetIterator extends TargetSetIterator
		{
			void filter()
			{
				while (true)
				{
					HGHandle h = currLink.getTargetAt(pos);
					if (!focus_seen && h.equals(src))
					{
						focus_seen = true;
						if (returnSource && siblingPredicate.satisfies(hg, src))
							return;
						else if (!returnSucceeding)
						{
							pos = -1;
							return;
						}
					}
					else if (siblingPredicate.satisfies(hg, h))
						return;
					if (--pos == -1)
					{
						return;
					}
				}				
			}
			
			void reset()
			{
				pos = currLink.getArity() - 1;
				focus_seen = false;
				if (!returnPreceeding)
				{
					while (!currLink.getTargetAt(pos--).equals(src));
					focus_seen = true;
					if (returnSource && (siblingPredicate == null || siblingPredicate.satisfies(hg, src)))
					{
						pos++;
						return;
					}
					else if (pos == -1)
						return;										
				}
				if (siblingPredicate != null)
					filter();		
				else if (!focus_seen && currLink.getTargetAt(pos).equals(src))
				{
					focus_seen = true;
					if (returnSource && (siblingPredicate == null || siblingPredicate.satisfies(hg, src)))
						return;
					else if (!returnSucceeding)
					{
						pos = -1;
						return;
					}
					else
						pos--;
				}				
			}
			
			void advance()
			{
				if (--pos == -1)
					return;
				else if (siblingPredicate != null)
					filter();
				else if (!focus_seen && currLink.getTargetAt(pos).equals(src))
				{
					focus_seen = true;
					if (returnSource)
						return;
					if (!returnSucceeding)
						pos = -1;
					else
						pos--;
				}
			}
		}
		
		void getNextLink()
		{
			// loop makes sure that we skip links that only point to our 'src' atom and nothing else
			while (true)
			{
				if (!linksIterator.hasNext())
				{
					currLink = null;
					if (closeResultSet)
						((HGSearchResult<HGHandle>)linksIterator).close();
					return;
				}
				hCurrLink = linksIterator.next();
				if (linkPredicate != null && !linkPredicate.satisfies(hg, hCurrLink))
					continue;
				if (hg.isLoaded(hCurrLink))
					currLink = (HGLink)hg.get(hCurrLink);
				else
				{
					tempLink.setHandleArray(hg.getStore().getLink(hg.getPersistentHandle(hCurrLink)), 2);
					currLink = tempLink;
				}
				if (currLink.getArity() < minArity)
					continue;
				tsIter.reset();
				if (tsIter.hasNext())
					break;
			}
		}
				
		public AdjIterator(HGHandle src, Iterator<HGHandle> linksIterator, boolean closeResultSet)
		{
			this.src = src;
			this.linksIterator = linksIterator;
			this.closeResultSet = closeResultSet;
			if (reverseOrder)
				tsIter = new BTargetSetIterator();
			else
				tsIter = new FTargetSetIterator();
			if (returnSource)
				minArity = 1;
			getNextLink();
		}
		
		public void remove() { throw new UnsupportedOperationException(); }
		
		public boolean hasNext()
		{
			return currLink != null;
		}
		
		public Pair<HGHandle, HGHandle> next()
		{		    
			current = new Pair<HGHandle, HGHandle>(hCurrLink, tsIter.next());
			if (!tsIter.hasNext())
				getNextLink();			
			return current;
		}
		
		public void close()
		{
			if (closeResultSet)
				((HGSearchResult<HGHandle>)linksIterator).close();			
		}

		public Pair<HGHandle, HGHandle> current()
		{
			return current;
		}

		public boolean isOrdered()
		{
			return false;
		}

		public boolean hasPrev() { throw new UnsupportedOperationException(); }
		public Pair<HGHandle, HGHandle> prev() { throw new UnsupportedOperationException(); }		
	}
	
	/**
	 * <p>
	 * Default constructor available, but the class is not really default constructible - 
	 * you must at least specify a <code>HyperGraph</code> instance on which to operate.
	 * </p>
	 */
	public DefaultALGenerator()
	{		
	}

	/**
	 * <p>
	 * Construct with default values: no link or sibling predicate, returning all
	 * siblings in their normal storage order.
	 * </p>
	 */
	public DefaultALGenerator(HyperGraph graph)
	{
		this.hg = graph;
	}
	
	/**
	 * <p>
	 * Construct a default adjacency list generator where links are considered <strong>unordered</strong>.
	 * </p>
	 * 
	 * @param hg The HyperGraph instance from where incidence sets are fetched.
	 * @param linkPredicate The predicate by which links are filtered. Only links satisfying
	 * this predicate will be considered. If this parameter is <code>null</code>, all links
	 * from the incidence set will be considered.
	 * @param siblingPredicate The predicate by which sibling atoms are filtered from the
	 * adjacency list. Only atoms satisfying this predicate will be returned. If this parameter
	 * is <code>null</code>, all sibling atoms will be considered. 
	 */
	public DefaultALGenerator(HyperGraph hg, 
							  HGAtomPredicate linkPredicate,
							  HGAtomPredicate siblingPredicate)
	{
		this.hg = hg;
		this.linkPredicate = linkPredicate;
		this.siblingPredicate = siblingPredicate;
	}

	/**
	 * <p>
	 * Construct a default adjacency list generator where links are considered <strong>ordered</strong>.
	 * </p>
	 *
	 * <p>
	 * The constructor does NOT allow both <code>returnSucceeding</code> and <code>returnPreceeding</code>
	 * to be set to <code>false</code>. This will always return empty adjacency lists and does not make
	 * any sense. Even, in a more complex situation where those parameters are determined at run-time 
	 * following some unforeseen logic, the caller must make sure that not both of those parameters are
	 * false.
	 * </p>
	 * 
	 * @param hg The HyperGraph instance from where incidence sets are fetched.
	 * @param linkPredicate The predicate by which links are filtered. Only links satisfying
	 * this predicate will be considered. If this parameter is <code>null</code>, all links
	 * from the incidence set will be considered.
	 * @param siblingPredicate The predicate by which sibling atoms are filtered from the
	 * adjacency list. Only atoms satisfying this predicate will be returned. If this parameter
	 * is <code>null</code>, all sibling atoms will be considered.
	 * @param returnPreceeding Whether or not to return siblings that appear before the focus 
	 * atom in an ordered link.
	 * @param returnSucceding Whether or not to return siblings that appear after the focus atom
	 * in an ordered link.
	 * @param reverseOrder Whether or not to reverse the default order implied by a link's target
	 * array. Note that this parameter affects the meaning of <em>preceeding</em> and <em>succeeding</em>
	 * in the above two parameters.
	 */
	public DefaultALGenerator(HyperGraph hg, 
							  HGAtomPredicate linkPredicate,
							  HGAtomPredicate siblingPredicate,
							  boolean returnPreceeding,
							  boolean returnSucceeding,
							  boolean reverseOrder)
	{
		this.hg = hg;
		this.linkPredicate = linkPredicate;
		this.siblingPredicate = siblingPredicate;
		this.returnPreceeding = returnPreceeding;
		this.returnSucceeding = returnSucceeding;
		this.reverseOrder = reverseOrder;
		
		if (!returnPreceeding && !returnSucceeding)
			throw new HGException("DefaultALGenerator: attempt to construct with both returnSucceeding and returnPreceeding set to false.");
	}

	/**
	 * <p>
	 * Construct a default adjacency list generator where links are considered <strong>ordered</strong>.
	 * </p>
	 *
	 * <p>
	 * The constructor does NOT allow both <code>returnSucceeding</code> and <code>returnPreceeding</code>
	 * to be set to <code>false</code>. This will always return empty adjacency lists and does not make
	 * any sense. Even, in a more complex situation where those parameters are determined at run-time 
	 * following some unforeseen logic, the caller must make sure that not both of those parameters are
	 * false.
	 * </p>
	 * 
	 * @param graph The HyperGraph instance from where incidence sets are fetched.
	 * @param linkPredicate The predicate by which links are filtered. Only links satisfying
	 * this predicate will be considered. If this parameter is <code>null</code>, all links
	 * from the incidence set will be considered.
	 * @param siblingPredicate The predicate by which sibling atoms are filtered from the
	 * adjacency list. Only atoms satisfying this predicate will be returned. If this parameter
	 * is <code>null</code>, all sibling atoms will be considered.
	 * @param returnPreceeding Whether or not to return siblings that appear before the focus 
	 * atom in an ordered link.
	 * @param returnSucceding Whether or not to return siblings that appear after the focus atom
	 * in an ordered link.
	 * @param reverseOrder Whether or not to reverse the default order implied by a link's target
	 * array. Note that this parameter affects the meaning of <em>preceeding</em> and <em>succeeding</em>
	 * in the above two parameters.
	 * @param returnSource Whether to return the source/originating atom along with its siblings. The default
	 * is false.
	 */
	public DefaultALGenerator(HyperGraph graph, 
							  HGAtomPredicate linkPredicate,
							  HGAtomPredicate siblingPredicate,
							  boolean returnPreceeding,
							  boolean returnSucceeding,
							  boolean reverseOrder,
							  boolean returnSource)
	{
		this.hg = graph;
		this.linkPredicate = linkPredicate;
		this.siblingPredicate = siblingPredicate;
		this.returnPreceeding = returnPreceeding;
		this.returnSucceeding = returnSucceeding;
		this.reverseOrder = reverseOrder;
		this.returnSource = returnSource;
//		if (!returnPreceeding && !returnSucceeding && !returnSource)
//			throw new HGException("DefaultALGenerator: attempt to construct with both returnSucceeding and returnPreceeding set to false.");
	}
	
	public HGSearchResult<Pair<HGHandle, HGHandle>> generate(HGHandle h) 
	{		
		return new AdjIterator(h, 
							   hg.getIncidenceSet(h).getSearchResult(), 
							   true);
	}
	
	public void close()
	{
		if (currIterator != null && currIterator.closeResultSet)
			((HGSearchResult<HGHandle>)currIterator.linksIterator).close();
	}

	public HyperGraph getGraph()
	{
		return hg;
	}

	public DefaultALGenerator setGraph(HyperGraph graph)
	{
		this.hg = graph;
		return this;
	}

	public HGAtomPredicate getLinkPredicate()
	{
		return linkPredicate;
	}

	public DefaultALGenerator setLinkPredicate(HGAtomPredicate linkPredicate)
	{
		this.linkPredicate = linkPredicate;
		return this;
	}

	public HGAtomPredicate getSiblingPredicate()
	{
		return siblingPredicate;
	}

	public DefaultALGenerator setSiblingPredicate(HGAtomPredicate siblingPredicate)
	{
		this.siblingPredicate = siblingPredicate;
		return this;
	}

	public boolean isReturnPreceeding()
	{
		return returnPreceeding;
	}

	public DefaultALGenerator setReturnPreceeding(boolean returnPreceeding)
	{
		this.returnPreceeding = returnPreceeding;
		return this;
	}

	public boolean isReturnSucceeding()
	{
		return returnSucceeding;
	}

	public DefaultALGenerator setReturnSucceeding(boolean returnSucceeding)
	{
		this.returnSucceeding = returnSucceeding;
		return this;
	}

	public boolean isReverseOrder()
	{
		return reverseOrder;
	}

	public DefaultALGenerator setReverseOrder(boolean reverseOrder)
	{
		this.reverseOrder = reverseOrder;
		return this;
	}

	public boolean isReturnSource()
	{
		return returnSource;
	}

	public DefaultALGenerator setReturnSource(boolean returnSource)
	{
		this.returnSource = returnSource;
		return this;
	}		
}