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
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * <code>TraversalResult</code> wraps a graph <code>HGTraversal</code> as
 * a query <code>HGSearchResult</code>. Because graph traversals are not bidirectional,
 * this implementation will throw an <code>UnsupportedOperationException</code> from
 * the backwards moving methods.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class TraversalResult implements HGSearchResult<Pair<HGHandle,HGHandle>> 
{
	private HGTraversal traversal;
	private Pair<HGHandle,HGHandle> current;
	
	public TraversalResult(HGTraversal traversal)
	{
		this.traversal = traversal;
		current = null;
	}
	
	public Pair<HGHandle,HGHandle> current() 
	{
		if (current == null)
			throw new NoSuchElementException();
		else
			return current;
	}

	public void close() 
	{
		// nothing to do
	}

	public boolean isOrdered() 
	{
		return false;
	}

	public boolean hasPrev() 
	{
		throw new UnsupportedOperationException();
	}

	public Pair<HGHandle,HGHandle> prev() 
	{
		throw new UnsupportedOperationException();
	}

	public boolean hasNext() 
	{
		return traversal.hasNext();
	}

	public Pair<HGHandle,HGHandle> next() 
	{
		current = traversal.next();
		return current;
	}

	public void remove() 
	{
		traversal.remove();
	}
}
