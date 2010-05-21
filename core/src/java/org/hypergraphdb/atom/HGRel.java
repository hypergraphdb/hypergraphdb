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
 * Represents a name relationship/link between entities.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGRel extends HGPlainLink 
{
	private String name;
	
	public HGRel(HGHandle...targetSet)
	{
		super(targetSet);
		this.name = "<name unavailable>";
	}
	
	public HGRel(String name, HGHandle...targetSet)
	{
		super(targetSet);
		this.name = name;
	}

	public String getName() 
	{
		return name;
	}
	
	public String toString()
	{
		return name + "[" + getArity() + "]";
	}
}
