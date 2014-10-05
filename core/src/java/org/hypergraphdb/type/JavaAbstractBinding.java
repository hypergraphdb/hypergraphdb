/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Iterator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * Acts as an atom type for Java interfaces and abstract classes that have
 * declared/visible bean properties or private fields translated into 
 * record slots, but cannot be instantiateed. Concrete
 * bean classes are represented by the <code>JavaBeanBinding</code> or the
 * <code>JavaObjectBinding</code> implementations depending on how exactly
 * that particular Java class is handled. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class JavaAbstractBinding implements HGCompositeType 
{
    protected HyperGraph graph;	
	protected Class<?> javaClass;
    protected HGHandle typeHandle;
    protected HGCompositeType hgType;    
    
    public JavaAbstractBinding(HGHandle typeHandle, HGCompositeType hgType, Class<?> clazz)
    {
    	this.typeHandle = typeHandle;
    	this.javaClass = clazz;
    	this.hgType = hgType;
    }
    
    public void setHyperGraph(HyperGraph hg)
    {
        this.graph = hg;    	
        hgType.setHyperGraph(hg);
    }
    
    public Class<?> getJavaClass()
    {
    	return javaClass;
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
		HGProjection p = hgType.getProjection(dimensionName);
		if (p == null)
			throw new HGException("Could not find projection for '" + dimensionName + 
					"' in HG type " + typeHandle + " for " + javaClass.getName());
		else
			return new BeanPropertyBasedProjection(p);
	}
    
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		throw new HGException("Cannot create a run-time instance of a HGAbstractType:" + javaClass.getName());
	}

	public HGPersistentHandle store(Object instance) 
	{
		throw new HGException("Cannot store and instance of a HGAbstractType in the database:" + javaClass.getName());		
	}

	public void release(HGPersistentHandle handle) 
	{
		throw new HGException("Cannot release an instance of a HGAbstractType:" + javaClass.getName());		
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
            JavaAbstractBinding otherJB = (JavaAbstractBinding)other;
            //TODO:??? could be subclasses; if || with both forms of isAssignable is more correct
            return typeHandle.equals(otherJB.typeHandle) && javaClass.isAssignableFrom(otherJB.javaClass);            
        }
    }
    
    public int hashCode()
    {
    	return HGUtils.hashIt(this.typeHandle);
    }
    
    public String toString()
    {
    	return javaClass.toString();
    }
}
