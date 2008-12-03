package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.ComparisonOperator;

public class ValueAsPredicateOnly extends AtomValueCondition
{
    public ValueAsPredicateOnly(Object value, ComparisonOperator operator)
    {
    	super(value, operator);
    }
}