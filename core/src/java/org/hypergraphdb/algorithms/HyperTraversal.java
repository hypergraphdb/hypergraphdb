/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import java.util.HashSet;
import java.util.Set;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.TargetSetIterator;

/**
 * 
 * <p>
 * This class is work in progress - it was done to solve the immediate problem
 * of transferring a sub-graph from one location to another. But a fully general
 * and clean notion of a "hyper-traversal" is yet to be defined....logged as an
 * issue on the project pages.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HyperTraversal implements HGTraversal, HGGraphHolder
{
    private transient HyperGraph graph;
    private HGTraversal flatTraversal;
    private HGAtomPredicate linkPredicate;
    private Set<HGHandle> visited = new HashSet<HGHandle>();
    private TargetSetIterator titer = null;
    private HGHandle currentLink = null;
    
    public HyperTraversal()
    {
        
    }
    
    public HyperTraversal(HyperGraph graph, HGTraversal flatTraversal)
    {
        this.graph = graph;
        this.flatTraversal = flatTraversal;
    }

    public HyperTraversal(HyperGraph graph, HGTraversal flatTraversal, HGAtomPredicate linkPredicate)
    {
        this.graph = graph;
        this.flatTraversal = flatTraversal;
        this.linkPredicate = linkPredicate;
    }
    
    public boolean hasNext()
    {
        if (currentLink == null || !titer.hasNext())
            return flatTraversal.hasNext();
        else
            return true;
    }

    public boolean isVisited(HGHandle handle)
    {
        return visited.contains(handle) || flatTraversal.isVisited(handle);
    }

    public Pair<HGHandle, HGHandle> next()
    {
        if (currentLink != null && titer.hasNext())
            return new Pair<HGHandle, HGHandle>(currentLink, titer.next());
        Pair<HGHandle, HGHandle> p = flatTraversal.next();
        Object atom = graph.get(p.getSecond());
        if (atom instanceof HGLink && 
            (linkPredicate == null || linkPredicate.satisfies(graph, p.getSecond())))
        {
            currentLink = p.getSecond();
            titer = new TargetSetIterator((HGLink)graph.get(currentLink));
        }
        else
        {
            currentLink = null;
            titer = null;
        }
        return p;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
    
    public HGTraversal getFlatTraversal()
    {
        return flatTraversal;
    }
    
    public void setFlatTraversal(HGTraversal flatTraversal)
    {
        this.flatTraversal = flatTraversal;
    }

    public HGAtomPredicate getLinkPredicate()
    {
        return linkPredicate;
    }

    public void setLinkPredicate(HGAtomPredicate linkPredicate)
    {
        this.linkPredicate = linkPredicate;
    }

    public void setHyperGraph(HyperGraph graph)
    {
        this.graph = graph;
    }
    
    public HyperGraph getHyperGraph()
    {
        return this.graph;
    }
}
