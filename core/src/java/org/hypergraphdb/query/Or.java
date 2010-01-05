/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import java.util.ArrayList;
import java.util.Iterator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * Represents the disjunction operator of a query condition. Several sub-clauses
 * can be combined with an <code>or</code> operator using this class.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class Or extends ArrayList<HGQueryCondition> implements HGQueryCondition, HGAtomPredicate 
{
	private static final long serialVersionUID = -1;	
	
	public Or()
	{		
	}
	
	public Or(HGQueryCondition clause)
	{
		add(clause);
	}

	public Or(HGQueryCondition clause1, HGQueryCondition clause2)
	{
		add(clause1);
		add(clause2);		
	}

	public Or(HGQueryCondition clause1, HGQueryCondition clause2, HGQueryCondition clause3)
	{
		add(clause1);
		add(clause2);
		add(clause3);		
	}

	public Or(HGQueryCondition clause1, HGQueryCondition clause2, HGQueryCondition clause3, HGQueryCondition clause4)
	{
		add(clause1);
		add(clause2);
		add(clause3);
		add(clause4);		
	}

	public Or(HGQueryCondition clause1, HGQueryCondition clause2, HGQueryCondition clause3, HGQueryCondition clause4, HGQueryCondition clause5)
	{
		add(clause1);
		add(clause2);
		add(clause3);
		add(clause4);
		add(clause5);		
	}
		
	public boolean satisfies(HyperGraph hg, HGHandle value)
	{
        for (int i = 0; i < size(); i++)
        {
        	HGQueryCondition c = get(i);
        	if (! (c instanceof HGAtomPredicate))
        		throw new Error("Attempt to use Or as a predicate where the sub-condition " + c + " is not a predicate.");
            if (((HGAtomPredicate)c).satisfies(hg, value))
                return true;
        }
        return false;
	} 
	
	public Object clone()
	{
		Or result = new Or();
		result.addAll(this);
		return result;
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("Or(");
		for (Iterator<HGQueryCondition> i = iterator(); i.hasNext(); )
		{
			result.append(i.next().toString());
			if (i.hasNext())
				result.append(", ");
		}
		result.append(")");
		return result.toString();
	}
	
	public boolean equals(Object x)
	{
		if ( ! (x instanceof Or))
			return false;
		else
			return super.equals((Or)x);
	}	
}
