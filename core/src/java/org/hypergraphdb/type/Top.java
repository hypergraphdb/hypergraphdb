/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * The <code>Top HGAtomType</code> represents the type of predefined types. It is
 * the top of the hypergraph type tower. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class Top implements HGAtomType
{
    private HyperGraph hg;
    
    private static final Top instance = new Top();
    
    public Top()
    {        
    }
    
    public static Top getInstance()
    {
        return instance;
    }
    
    public void setHyperGraph(HyperGraph hg)
    {
        this.hg = hg;
    }

    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {
        return hg.getTypeSystem().getType(handle);
    }

    public HGPersistentHandle store(Object instance)
    {
        throw new HGException("Top.store: can't store predefined types, that's why they are predefined.");
    }

    public void release(HGPersistentHandle handle)
    {
        throw new HGException("Top.store: can't store release predefined types, that's why they are predefined.");        
    }

    public boolean subsumes(Object general, Object specific)
    {
        return general == null && specific == null ||
               general != null && specific != null && general.equals(specific); 
    }
}
