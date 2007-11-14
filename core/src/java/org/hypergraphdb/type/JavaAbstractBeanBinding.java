/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.util.Iterator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * Acts as an atom type for Java interfaces and abstract classes that have
 * declared/visible bean properties, but cannot be instantiateed. Concrete
 * bean classes are represented by the <code>JavaBeanBinding</code> implementation. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class JavaAbstractBeanBinding implements HGCompositeType 
{
    protected HyperGraph graph;	
	protected Class<?> beanClass;
    protected HGHandle typeHandle;
    protected HGCompositeType hgType;    
    
    public JavaAbstractBeanBinding(HGHandle typeHandle, HGCompositeType hgType, Class<?> clazz)
    {
    	this.typeHandle = typeHandle;
    	this.beanClass = clazz;
    	this.hgType = hgType;
    }
    
    public void setHyperGraph(HyperGraph hg)
    {
        this.graph = hg;    	
        hgType.setHyperGraph(hg);
    }
    
    public HGCompositeType getHGType()
    {
    	return hgType;
    }

    public HGHandle getTypeHandle()
    {
    	return typeHandle;
    }
    
	public Iterator<String> getDimensionNames() 
	{
		return hgType.getDimensionNames();
	}

	public HGProjection getProjection(String dimensionName) 
	{
		return new BeanPropertyBasedProjection(hgType.getProjection(dimensionName));
	}
    
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		throw new HGException("Cannot create a run-time instance of a HGAbstractType.");
	}

	public HGPersistentHandle store(Object instance) 
	{
		throw new HGException("Cannot store and instance of a HGAbstractType in the database.");		
	}

	public void release(HGPersistentHandle handle) 
	{
		throw new HGException("Cannot release an instance of a HGAbstractType.");		
	}
	
    public boolean subsumes(Object general, Object specific)
    {
		return general.getClass().isAssignableFrom(specific.getClass());
    }
    
    public boolean equals(Object other)
    {
        if (! (other instanceof JavaBeanBinding))
            return false;
        else
        {
            JavaAbstractBeanBinding otherJB = (JavaAbstractBeanBinding)other;
            //TODO:??? could be subclasses; if || with both forms of isAssignable is more correct
            return typeHandle.equals(otherJB.typeHandle) && beanClass.isAssignableFrom(otherJB.beanClass);            
        }
    }
    
    public String toString()
    {
    	return beanClass.toString();
    }
}