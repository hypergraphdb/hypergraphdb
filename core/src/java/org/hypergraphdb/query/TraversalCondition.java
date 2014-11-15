/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGALGenerator;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * 
 * <p>
 * A condition that gets translated into a graph traversal. This is base class for 
 * the two variants of traversal (breadth-first and depth-first). A traversal condition
 * will generally be configured with more than just the starting atom. It will use
 * a {@link DefaultALGenerator} for the traversal. So for the meaning of parameters
 * such as <code>linkPredicate</code>, <code>returnPreceeding</code> etc., see 
 * {@link DefaultALGenerator}.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public abstract class TraversalCondition implements HGQueryCondition
{
	private Ref<HGHandle> startAtom;
	private HGAtomPredicate linkPredicate = null;
	private HGAtomPredicate siblingPredicate = null;
	private boolean returnPreceeding = true, 
				    returnSucceeding = true, 
				    reverseOrder = false,
				    returnSource = false;

	public TraversalCondition()
	{
		
	}
	
	public TraversalCondition(HGHandle startAtom)
	{
		this.startAtom = hg.constant(startAtom);
	}

	public TraversalCondition(Ref<HGHandle> startAtom)
	{
		this.startAtom = startAtom;
	}
	
	public HGALGenerator makeGenerator(HyperGraph graph)
	{
		return new DefaultALGenerator(graph, 
									  linkPredicate, 
									  siblingPredicate, 
									  returnPreceeding,
									  returnSucceeding,
									  reverseOrder,
									  returnSource);
	}
	
	public abstract HGTraversal getTraversal(HyperGraph graph);
	
	public Ref<HGHandle> getStartAtomReference()
	{
		return startAtom;
	}
	
	public void setStartAtomReference(Ref<HGHandle> startAtom)
	{
		this.startAtom = startAtom;
	}
	
	public HGHandle getStartAtom()
	{
		return startAtom == null ? null : startAtom.get();
	}

	public void setStartAtom(HGHandle startAtom)
	{
		this.startAtom = hg.constant(startAtom);
	}

	public HGAtomPredicate getLinkPredicate()
	{
		return linkPredicate;
	}

	public void setLinkPredicate(HGAtomPredicate linkPredicate)
	{
		this.linkPredicate = linkPredicate;
	}

	public HGAtomPredicate getSiblingPredicate()
	{
		return siblingPredicate;
	}

	public void setSiblingPredicate(HGAtomPredicate siblingPredicate)
	{
		this.siblingPredicate = siblingPredicate;
	}

	public boolean isReturnPreceeding()
	{
		return returnPreceeding;
	}

	public void setReturnPreceeding(boolean returnPreceeding)
	{
		this.returnPreceeding = returnPreceeding;
	}

	public boolean isReturnSucceeding()
	{
		return returnSucceeding;
	}

	public void setReturnSucceeding(boolean returnSucceeding)
	{
		this.returnSucceeding = returnSucceeding;
	}

	public boolean isReverseOrder()
	{
		return reverseOrder;
	}

	public void setReverseOrder(boolean reverseOrder)
	{
		this.reverseOrder = reverseOrder;
	}

	public boolean isReturnSource()
	{
		return returnSource;
	}

	public void setReturnSource(boolean returnSource)
	{
		this.returnSource = returnSource;
	}
	
	public int hashCode() 
	{ 
		return HGUtils.hashThem(startAtom, 
								HGUtils.hashThem(linkPredicate, 
												 HGUtils.hashThem(siblingPredicate, reverseOrder)));
	}
	
	public boolean equals(Object x)
	{
		if (x == null)
			return false;
		else if (!x.getClass().equals(this.getClass()))
			return false;
		else
		{
			TraversalCondition c = (TraversalCondition)x;
			return HGUtils.eq(startAtom, c.startAtom) &&
				   HGUtils.eq(linkPredicate, c.linkPredicate) &&
				   HGUtils.eq(siblingPredicate, c.siblingPredicate) &&
				   returnSucceeding == c.returnSucceeding &&
				   returnPreceeding == c.returnPreceeding &&
				   returnSource == c.returnSource &&
				   reverseOrder == c.reverseOrder;
		}
	}	
}
