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

/**
 * <p>
 * This condition is satisfied by any and all atoms in the HyperGraph database.
 * That is, it is satisfied given <code>HGHandle</code> if and only if it is
 * an atom in the graph.
 * </p>
 *  
 * <p>
 * When translated to a query alone, it will result in an enumeration of all
 * atoms in the database. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AnyAtomCondition implements HGQueryCondition, HGAtomPredicate 
{
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		return hg.get(handle) != null;
	}
	
	public int hashCode() 
	{ 
		return 0; 
	}
	
	public boolean equals(Object x)
	{
		return x instanceof AnyAtomCondition;
	}
	
	public String toString()
	{
		return "any()";
	}
}
