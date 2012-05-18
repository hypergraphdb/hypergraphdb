/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * Basic class for conditions examining individual primitive values.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class SimpleValueCondition implements HGQueryCondition, HGAtomPredicate 
{
	protected Ref<Object> value;
	protected ComparisonOperator operator;
 	
	/**
	 * We are using the standard Java <code>java.lang.Comparable</code> interface here
	 * on the <code>value</code> parameter. The passed in type is ignored, but is
	 * part of the method signature because it is still not clear how exactly
	 * value orderings are to be treated. All Java primitive types are comparable and
	 * our primitive types are nothing more than the Java primitive types. Perhaps we
	 * need an "ordered" interface for atom types. Of course values can always implement
	 * the java.lang.Comparable, but this breaks HyperGraph paradigm where such semantics
	 * are defined by the types, not their values.
	 */
	protected boolean compareToValue(HyperGraph graph, Object x)
	{
		if (x instanceof HGValueLink)
			x = ((HGValueLink)x).getValue();
    	switch (operator)
    	{
    		case EQ:
    			return HGUtils.eq(value.get(), x);
    		case LT:
    			return ((Comparable)x).compareTo(value.get()) < 0;
    		case GT:
    			return ((Comparable)x).compareTo(value.get()) > 0;
    		case LTE:
    			return ((Comparable)x).compareTo(value.get()) <= 0;
    		case GTE:
    			return ((Comparable)x).compareTo(value.get()) >= 0;   
    		default:
    			throw new HGException("Wrong operator code [" + operator + "] passed to SimpleValueCondition.");
    	}
 	}
	
	public SimpleValueCondition()
	{
		
	}
	
	public SimpleValueCondition(Object value)
	{
		this(value, ComparisonOperator.EQ);
	}

	public SimpleValueCondition(Object value, ComparisonOperator operator)
	{
		this.value = hg.constant(value);
		this.operator = operator;
	}
    
	public SimpleValueCondition(Ref<Object> value, ComparisonOperator operator)
	{
		this.value = value;
		this.operator = operator;
	}
	
	public Ref<Object> getValueReference()
	{
		return value;
	}
	
	public void setValueReference(Ref<Object> value)
	{
		this.value = value;
	}
	
	public Object getValue()
	{
		return value == null ? null : value.get();
	}
	
	public void setValue(Object value)
	{
		this.value = hg.constant(value);
	}

	public ComparisonOperator getOperator()
	{
		return operator;
	}
	
	public void setOperator(ComparisonOperator operator)
	{
		this.operator = operator;
	}
	
	public int hashCode()
	{
		return HGUtils.hashIt(value);
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SimpleValueCondition))
			return false;
		SimpleValueCondition y = (SimpleValueCondition)x;
		return HGUtils.eq(value, y.value) && operator.equals(y.operator);
	}
}
