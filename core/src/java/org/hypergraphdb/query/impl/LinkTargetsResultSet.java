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
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGSearchResult;

/**
 * 
 * <p>
 * This is the same as {@link HandleArrayResultSet}, but it uses a loaded link atom instance
 * instead of a <code>HGHandle[]</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class LinkTargetsResultSet implements HGSearchResult<HGHandle>
{
	private HGLink l = null;
	private int pos = -1;
	
	public LinkTargetsResultSet(HGLink l)
	{
		if (l == null)
			throw new IllegalArgumentException("LinkTargetsResultSet with null link.");		
		this.l = l;
	}
	
	public void close()
	{
	}

	public HGHandle current()
	{
		if (pos < 0 || pos >= l.getArity())
			throw new NoSuchElementException();
		return	l.getTargetAt(pos);	
	}

	public boolean isOrdered()
	{
		return false;
	}

	public boolean hasPrev()
	{		
		return pos > 0;
	}

	public HGHandle prev()
	{
		if (pos <= 0)
			throw new NoSuchElementException();
		return l.getTargetAt(--pos);
	}

	public boolean hasNext()
	{
		return pos < l.getArity() - 1;
	}

	public HGHandle next()
	{
		if (pos >= l.getArity())
			throw new NoSuchElementException();
		return l.getTargetAt(++pos);
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
