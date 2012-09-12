/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGPersistentHandle;

/**
 * <p>
 * The <code>HGTypeStructuralInfo</code> class represents a HyperGraph atom that provides
 * fixed structural information about atoms of a definite type.</p>
 * <p>
 *  More often than not,
 * it is the case that atoms of a particular type will all have the same arity. In 
 * addition, links of the same type will all be ordered or all unordered (directed or
 * undirected when using the terminology for links of arity 2). This structural uniformity
 * is not enforced by HyperGraph, but it can be declared, for purposes such as applying better
 * visualization algorithms and query optimization, by adding a <code>HGTypeStructuralInfo</code>
 * atom with the relevant information.
 * </p>
 * <p>
 * If an application adds such structural information for a
 * HyperGraph type, it is expected to stick to it. That is, while being just a "hint", HyperGraph
 * will freely use this information assuming it is correct and it applies to all atoms of a
 * particular type. 
 * </p>
 * 
 * <p>
 * A <code>HGTypeStructuralInfo</code> is a simple record (a bean) with the following information:
 * 
 * <ul>
 * <li><b>typeHandle</b> - The persistent handle of the type to which this structural information applies.</li>
 * <li><b>arity</b>  - The fixed arity of all atoms of that particular type. A value of Integer.MAX_VALUE
 * indicates variable arity.</li>
 * <li><b>ordered</b> - Whether links of that type are ordered or not. Note that this flag
 * 					    applies only when <code>arity > 0</code>.</li>
 * </ul>
 * 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class HGTypeStructuralInfo 
{
	private int arity;
	private boolean ordered;
	private HGPersistentHandle typeHandle;
	
	/**
	 * <p>Default constructor.</p>
	 */
	public HGTypeStructuralInfo()
	{		
	}

	/**
	 * <p>Construct a <code>HGTypeStructuralInfo</code> with the given set of
	 * parameters.</p>
	 * 
	 * @param typeHandle The persistent handle of the type to which this structural 
	 * information applies.
	 * @param arity The fixed arity of all atoms of that particular type. 
	 * @param ordered Whether links of that type are ordered or not. Note that this flag
	 * applies only when <code>arity > 0</code>.
	 */
	public HGTypeStructuralInfo(HGPersistentHandle typeHandle, int arity, boolean ordered)
	{
		this.typeHandle = typeHandle;
		this.arity = arity;
		this.ordered = ordered;
	}

	public final int getArity() 
	{
		return arity;
	}

	public final void setArity(int arity) 
	{
		this.arity = arity;
	}

	public final boolean isOrdered() 
	{
		return ordered;
	}

	public final void setOrdered(boolean ordered) 
	{
		this.ordered = ordered;
	}

	public final HGPersistentHandle getTypeHandle() 
	{
		return typeHandle;
	}

	public final void setTypeHandle(HGPersistentHandle typeHandle) 
	{
		this.typeHandle = typeHandle;
	}
}