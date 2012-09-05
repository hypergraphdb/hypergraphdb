/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * Represents an index by a specific target position in ordered links.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class ByTargetIndexer extends HGKeyIndexer<HGPersistentHandle>
{
    private int target;

    public ByTargetIndexer()
    {
    }

    public ByTargetIndexer(HGHandle type, int target)
    {
        super(type);
        this.target = target;
    }

    public ByTargetIndexer(String name, HGHandle type, int target)
    {
        super(name, type);
        this.target = target;
    }

    public int getTarget()
    {
        return target;
    }

    public void setTarget(int target)
    {
        this.target = target;
    }

    public Comparator<byte[]> getComparator(HyperGraph graph)
    {
        return null;
    }

    public ByteArrayConverter<HGPersistentHandle> getConverter(HyperGraph graph)
    {
        return BAtoHandle.getInstance(graph.getHandleFactory());
    }

    public HGPersistentHandle getKey(HyperGraph graph, Object atom)
    {
        return graph.getPersistentHandle(((HGLink) atom).getTargetAt(target));
    }

    public boolean equals(Object other)
    {
        if (other == this)
            return true;
        if (!(other instanceof ByTargetIndexer))
            return false;
        ByTargetIndexer idx = (ByTargetIndexer) other;
        return getType().equals(idx.getType()) && idx.target == target;
    }

    public int hashCode()
    {
    		int hash = 7;
    		hash = 31 * hash + target;
    		hash = 31 * hash + getType().hashCode();
        return hash;
    }
}