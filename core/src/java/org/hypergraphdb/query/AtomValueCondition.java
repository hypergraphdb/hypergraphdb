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
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * The <code>AtomValueCondition</code> represents a query condition on
 * the atom value. The condition specifies a comparison operator and
 * value to compare against. The possible comparison operators are
 * the constants listed in the <code>ComparisonOperator</code> class. 
 * The value compared against must be of a recognizable <code>HGAtomType</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomValueCondition extends SimpleValueCondition 
{
   
	public AtomValueCondition()
	{
	   
	}
	
	public AtomValueCondition(Object value)
	{
		super(value);
	}

	public AtomValueCondition(Ref<Object> value, ComparisonOperator operator)
	{
		super(value, operator);
	}
	
	public AtomValueCondition(Object value, ComparisonOperator operator)
	{
		super(value, operator);
	}

	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		Object atom = null;
		atom = hg.get(handle);		
		if (atom == null)
			return false;
		else
			return compareToValue(hg, atom);
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("valueIs(");
		result.append(getOperator());
		result.append(",");
		result.append(String.valueOf(getValue()));
		result.append(")");
		return result.toString();
	}
	
	public int hashCode() 
	{ 
		return HGUtils.hashThem(value, operator);
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomValueCondition))
			return false;
		else
		{
			AtomValueCondition c = (AtomValueCondition)x;
			return HGUtils.eq(operator, c.operator) &&
				   HGUtils.eq(value, c.value);
		}
	}		
}
