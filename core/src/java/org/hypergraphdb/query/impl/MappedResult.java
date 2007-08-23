/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * A <code>MappedResult</code> is a <code>HGSearchResult</code> with an applied
 * transformation to each of its elements. The transformation is defined by 
 * implementing the <code>Mapping</code> interface.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class MappedResult implements HGSearchResult 
{
	private HGSearchResult r;
	private Mapping f;
	
	public MappedResult(HGSearchResult r, Mapping f)
	{
		this.r = r;
		this.f = f;
	}
	
	public Object current() 			{ return f.eval(r.current()); }
	public void close() 		    	{ r.close(); }
	public boolean hasPrev() 		    { return r.hasPrev(); }
	public Object prev() 			    { return f.eval(r.prev()); }
	public boolean hasNext() 			{ return r.hasNext(); }
	public Object next() 				{ return f.eval(r.next()); }
	public void remove() 				{ r.remove(); }
	public boolean isOrdered()			{ return false; }
}
