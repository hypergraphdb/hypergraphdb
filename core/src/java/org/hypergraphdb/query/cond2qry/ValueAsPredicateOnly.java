/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.util.Ref;

public class ValueAsPredicateOnly extends AtomValueCondition
{
	public ValueAsPredicateOnly(Ref<Object> value, ComparisonOperator operator)
	{
		super(value, operator);
	}
	
    public ValueAsPredicateOnly(Object value, ComparisonOperator operator)
    {
    	super(value, operator);
    }
}