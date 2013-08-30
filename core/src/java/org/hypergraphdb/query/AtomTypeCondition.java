/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * An <code>AtomTypeCondition</code> examines the type of a given atom
 * and evaluates to <code>true</code> or <code>false</code> depending on
 * whether the type of the atom matches exactly (i.e. has the same type 
 * handle) as that of the condition. Note that this condition looks at the
 * concrete type of the atom and ignores and sub-typing relationships. 
 * </p>
 * 
 * <p>
 * The type can be specified either as a HyperGraph handle or as a Java class.
 * In the latter, the corresponding HyperGraph will be retrieved at appropriate
 * times. However, those two properties overlap each other, in the sense the you
 * can set one or the other, but not both at the same time. If you set the type
 * as a Java class, the corresponding {@link HGHandle} will be retrieved every time
 * it is needed. In particular, if this is used as a filtering condition applied
 * iteratively over a large result set, performance will be hurt by the extra step.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomTypeCondition implements HGQueryCondition, HGAtomPredicate, TypeCondition 
{
    private Ref<?> type;
	
	public AtomTypeCondition()
	{
		
	}
	
	/**
	 * <p>Construct a new atom type condition.</p>
	 *  
	 * @param typeRef A {@link Ref} to a Java <code>Class</code> or a {@link HGHandle}.
	 */
	public AtomTypeCondition(Ref<?> typeRef)
	{
		this.type = typeRef;
	}
	
    public AtomTypeCondition(Class<?> javaClass)
    {
    	this(hg.constant(javaClass));
    }
    
	public AtomTypeCondition(HGHandle typeHandle)
	{
		this(hg.constant(typeHandle));
	}
	
	public HGHandle typeHandleIfAvailable(HyperGraph graph)
	{
        if (!hg.isVar(getTypeReference()))
        {
            if (getTypeHandle() != null)
                return getTypeHandle();
            else
                return graph.getTypeSystem().getTypeHandleIfDefined(getJavaClass());
        }	    
        else
            return null;
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
	
	public Ref<?> getTypeReference() 
	{
		return type;
	}
	
	public void setTypeReference(Ref<?> type)
	{
		this.type = type;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle  value)
	{
        HGHandle h = null;
        Object t = type.get();
        if (t == null)
        	throw new NullPointerException("AtomTypeCondition with null type.");
        else if (t instanceof HGHandle)
        	h = (HGHandle)t;
        else
        	h = hg.getTypeSystem().getTypeHandle((Class<?>)t);
        HGHandle typeOfValue = hg.getType(value);
        if (typeOfValue == null)
        	throw new HGException("Could not get type of atom " + value);
		return typeOfValue.equals(h);
	}
	
	public int hashCode() 
	{ 
		return  type == null ? 0 : HGUtils.hashIt(type.get());  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomTypeCondition))
			return false;
		else
		{
			AtomTypeCondition cond = (AtomTypeCondition)x;
			return type == null ? cond.type == null : 
				cond.type == null ? false : HGUtils.eq(type.get(), cond.type.get());
		}
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("typeIs(");
		result.append(getJavaClass() != null ? getJavaClass().getName() : getTypeHandle().getPersistent());
		result.append(")");
		return result.toString();
	}
}