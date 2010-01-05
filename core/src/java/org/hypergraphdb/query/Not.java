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
 * A generic negating <code>HGQueryCondition</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class Not implements HGQueryCondition, HGAtomPredicate 
{
	private HGAtomPredicate negated;
	
	public Not()
	{
		
	}
	public Not(HGAtomPredicate negated)
	{
		this.negated = negated;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle  value)
	{
		return !negated.satisfies(hg, value);
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("Not(");
		result.append(negated.toString());
		result.append(")");
		return result.toString();
	}

	public int hashCode() 
	{ 
		return ~negated.hashCode();  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof Not))
			return false;
		else
			return ((Not)x).negated.equals(negated);
	}
	
	public HGAtomPredicate getPredicate()
	{
		return negated;
	}
	public void setPredicate(HGAtomPredicate predicate)
	{
		this.negated = predicate;
	}
}
