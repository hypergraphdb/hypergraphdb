/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.CloseMe;

/**
 * 
 * <p>
 * Make a random access result set as a predicate. Random access result sets
 * don't always have a constant time (like "true" random access structures), but are always
 * assumed to have at least sublinear time. In other words, their goTo method
 * should have complexity smaller than O(N). In practice, RA sets are usually B-Tree 
 * or "B-Forests" accessed in O(logN). 
 * </p>
 * <p>
 * If an <code>RABasedPredicate</code> is initialized with a <code>HGQuery</code> 
 * instead of a result set, the query will be executed the first time <code>satisfies</code>
 * is called.
 * </p>
 * <p>
 * The <code>close</code> method will only close the result set if this <code>RABasedPredicate</code>
 * was created by calling the <code>HGQuery</code> based constructor.
 * </p>
 * @author Borislav Iordanov
 */
public class RABasedPredicate implements HGAtomPredicate, CloseMe 
{
	private HGRandomAccessResult ras = null;
	private HGQuery query;
	
	public RABasedPredicate(HGRandomAccessResult ras)
	{		
		this.ras = ras;
	}

	public RABasedPredicate(HGQuery query)
	{
		this.query = query;
	}
	
	/**
	 * <p>Return <code>true</code> if <code>handle</code> is a member of 
	 * the <code>HGRandomAccessResult</code> set on which this predicate
	 * is based. Return <code>false</code> otherwise.
	 */
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		if (ras == null)
			ras = (HGRandomAccessResult)query.execute();
		return ras.goTo(handle, true) == HGRandomAccessResult.GotoResult.found;
	}
	
	public void close()
	{
		if (query != null)
			try { ras.close(); } catch (Throwable t) { t.printStackTrace(System.err); }
	}
}
