/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.List;
import org.hypergraphdb.query.HGQueryCondition;

/**
 * <p>
 * A <code>HyperNode</code> abstracts the core {@link HyperGraph} interface for
 * manipulating data in the hypergraph model. Implementations can capture other
 * abstractions such a remote database instance, or a sub-graph (subset of a graph) or
 * a hypernode (i.e. an atom which is in turn a hypergraph). 
 * </p>
 */
public interface HyperNode
{
    <T> T get(HGHandle handle);
    
    public default HGHandle add(Object atom, HGHandle type)
    {
    	return add(atom, type);
    }
    
    HGHandle add(Object atom, HGHandle type, int flags);
    
    public void define(HGHandle handle,
                       HGHandle type,
                       Object instance,
                       int flags);

    /**
     * Delegate to {@link #define(HGHandle, HGHandle, Object, int)} with 0 as default flags.
     */
    public default void define(HGHandle atomHandle, HGHandle type, Object instance)
    {
    	this.define(atomHandle, type, instance, 0);
    }
    
    boolean remove(HGHandle handle);
    boolean replace(HGHandle handle, Object newValue, HGHandle newType);
    HGHandle getType(HGHandle handle);
    IncidenceSet getIncidenceSet(HGHandle handle);
    
    <T> T findOne(HGQueryCondition condition);
    <T> HGSearchResult<T> find(HGQueryCondition condition);
    <T> T getOne(HGQueryCondition condition);
    <T> List<T> getAll(HGQueryCondition condition);
    <T> List<T> findAll(HGQueryCondition condition);
    long count(HGQueryCondition condition);
}