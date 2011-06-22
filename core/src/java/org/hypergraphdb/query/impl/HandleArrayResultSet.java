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

/**
 * 
 * <p>
 * Implements a <code>HGSearchResult</code> comprising the atoms in a given HGHandle array.
 * </p>
 * 
 * <p>
 * Note: this is not an ordered result set, but can easily be made so if this proves
 * beneficial.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HandleArrayResultSet implements HGSearchResult<HGHandle>
{
	protected HGHandle [] A;
	protected int start, end;
	protected int pos = -1;
	
	public HandleArrayResultSet(HGHandle [] array)
	{
		this(array, 0, array.length);		
	}
	
	public HandleArrayResultSet(HGHandle [] array, int start)
	{
		this(array, start, array.length);
	}
	
	public HandleArrayResultSet(HGHandle [] array, int start, int end)
	{
		this.A = array;
		this.start = start;
		this.end = end;
		this.pos = start - 1;
	}
	
	public void close()
	{
	}

	public HGHandle current()
	{
		if (pos < start || pos >= end)
			throw new NoSuchElementException();
		return A[pos];
	}

	public boolean isOrdered()
	{
		return false;
	}

	public boolean hasPrev()
	{		
		return pos > start;
	}

	public HGHandle prev()
	{
		if (pos <= start)
			throw new NoSuchElementException();
		return A[--pos];
	}

	public boolean hasNext()
	{
		return pos < end - 1;
	}

	public HGHandle next()
	{
		if (pos >= end - 1)
			throw new NoSuchElementException();
		return A[++pos];
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
