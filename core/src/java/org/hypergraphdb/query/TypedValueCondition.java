/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * 
 * <p>
 * This is a <code>HGQueryCondition</code> that constrains the value of an
 * atom as well as its type. In general, the HyperGraph type system allows
 * values to have multiple types. Two atoms <code>X</code> and
 * <code>Y</code> can share the same value <code>V</code> without however
 * having the same type. <code>X</code> may be of type <code>T</code> and 
 * <code>Y</code> may be of type <code>S</code> different than <code>T</code>.
 * In such situations, when performing a query by value, a <code>TypedValueCondition<code>
 * should be used instead of <code>AtomValueCondition</code>. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class TypedValueCondition extends AtomValueCondition implements TypeCondition
{
	private Ref<?> type;

	public TypedValueCondition()
	{
		
	}
	
	public TypedValueCondition(Ref<?> type, Ref<Object> value)
	{
		this(type, value, ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(HGHandle typeHandle, Object value)
	{
		this(hg.constant(typeHandle), hg.constant(value), ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(Class<?> javaClass, Object value)
	{
		this(hg.constant(javaClass), hg.constant(value), ComparisonOperator.EQ);
	}
	
	public TypedValueCondition(Ref<?> type, Ref<Object> value, ComparisonOperator op)
	{
		super(value, op);
		this.type = type;
	}
	
	public TypedValueCondition(HGHandle typeHandle, Object value, ComparisonOperator op)
	{
		this(hg.constant(typeHandle), hg.constant(value), op);
	}

	public TypedValueCondition(Class<?> javaClass, Object value, ComparisonOperator op)
	{
		this(hg.constant(javaClass), hg.constant(value), op);
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		Object atom = hg.get(handle);		
		if (atom == null)
			return false;
		HGHandle atomType = hg.getTypeSystem().getTypeHandle(handle);		
        HGHandle typeHandle = null;
        Object t = type.get();
        if (t == null)
        	throw new NullPointerException("AtomTypeCondition with null type.");
        else if (t instanceof HGHandle)
        	typeHandle = (HGHandle)t;
        else
        	typeHandle = hg.getTypeSystem().getTypeHandle((Class)t);		
		return atomType.equals(typeHandle) && compareToValue(hg, atom); 
	}
		
	public Ref<?> getTypeReference() 
	{
		return type;
	}
	
	public void setTypeReference(Ref<?> type)
	{
		this.type = type;
	}
	
	public void setJavaClass(Class<?> c)
	{
		this.type = hg.constant(c);
	}
	
    public Class<?> getJavaClass()
    {
        Object t = type.get();
        return t instanceof Class ? (Class<?>)t : null;
    }
    
    public void setTypeHandle(HGHandle handle)
    {
    	this.type = hg.constant(handle); 
    }
    
	public HGHandle getTypeHandle()
	{
        Object t = type.get();
        return t instanceof HGHandle ? (HGHandle)t : null;
	}
	
	public HGHandle getTypeHandle(HyperGraph graph)
	{
		Object t = type.get();
		if (t instanceof HGHandle)
			return (HGHandle)t;
		else
			return graph.getTypeSystem().getTypeHandle((Class<?>)t);
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("valueIs(");
		result.append(getOperator());
		result.append(",");
		result.append(String.valueOf(getValue()));
		result.append(" with type ");
		result.append(getJavaClass() != null ? getJavaClass().getName() : getTypeHandle().getPersistent());
		result.append(")");
		return result.toString();
	}
	
	public int hashCode() 
	{ 
		return  super.hashCode() +
				(type == null ? 0 : HGUtils.hashIt(type.get()));  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof TypedValueCondition))
			return false;
		else if (!super.equals(x))
			return false;
		else
		{			
			TypedValueCondition c = (TypedValueCondition)x;
			return type == null ? c.type == null : 
				c.type == null ? false : HGUtils.eq(type.get(), c.type.get());
		}
	}	
}