/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
/*
 * Created on Aug 11, 2005
 *
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;

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
 * times.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomTypeCondition implements HGQueryCondition, HGAtomPredicate 
{
    private Class<?> c;
	private HGHandle handle;
	
	public AtomTypeCondition()
	{
		
	}
	
    public AtomTypeCondition(Class<?> javaClass)
    {
        if (javaClass == null)
            throw new NullPointerException();
        this.c = javaClass;
    }
    
	public AtomTypeCondition(HGHandle typeHandle)
	{
        if (typeHandle == null)
            throw new NullPointerException();
		this.handle = typeHandle;
	}
	
	public void setJavaClass(Class<?> c)
	{
		this.c = c;
	}
	
    public Class<?> getJavaClass()
    {
        return c;
    }
    
    public void setTypeHandle(HGHandle handle)
    {
    	this.handle = handle;
    }
    
	public HGHandle getTypeHandle()
	{
		return handle;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle  value)
	{
        HGHandle h = handle;
        if (h == null)
            h = hg.getTypeSystem().getTypeHandle(c);
        HGHandle typeOfValue = hg.getType(value);
        if (typeOfValue == null)
        	throw new HGException("Could not get type of atom " + value);
		return typeOfValue.equals(h);
	}
	
	public int hashCode() 
	{ 
		return  c == null ? handle.hashCode() : c.hashCode();  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomTypeCondition))
			return false;
		else
		{
			AtomTypeCondition cond = (AtomTypeCondition)x;
			return c == null ? HGUtils.eq(handle, cond.handle) : HGUtils.eq(c, cond.c);
		}
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("typeIs(");
		result.append(c != null ? c.getName() : handle);
		result.append(")");
		return result.toString();
	}
}