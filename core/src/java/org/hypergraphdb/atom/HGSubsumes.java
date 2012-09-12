/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;


/**
 * <p>
 * The <code>HGSubsumes</code> link represents a <em>subsumes</em> relationship between
 * two atoms, either declared, or inferred by HyperGraph. Generally, such a relationship
 * will exist between type atoms (i.e. instance of <code>HGAtomType</code>). One can
 * explicitly create such a link, for instance to declare a subtyping relationship
 * between types. HyperGraph is also allowed to create such links as part of query
 * processing or other activities.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGSubsumes extends HGPlainLink 
{
	public HGSubsumes(HGHandle [] link)
	{
		super(link);		
		if (link.length != 2)
			throw new IllegalArgumentException("The HGHandle [] passed to the HGSubsumes constructor must be of length 2.");
	}
	
	public HGSubsumes(HGHandle general, HGHandle specific)
	{
		super(new HGHandle[] { general, specific});
	}
	
	public HGHandle getGeneral()
	{
		return getTargetAt(0);
	}
	
	public HGHandle getSpecific()
	{
		return getTargetAt(1);
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("subsumes(");
		result.append(getGeneral());
		result.append(",");
		result.append(getSpecific());
		result.append(")");
		return result.toString();
	}
}
