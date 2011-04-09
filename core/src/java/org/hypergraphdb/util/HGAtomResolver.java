package org.hypergraphdb.util;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperNode;

public class HGAtomResolver<T> implements RefResolver<HGHandle, T>
{
    HyperNode node;
    
    public HGAtomResolver(HyperNode graph)
    {
        this.node = graph;
    }
    
    @SuppressWarnings("unchecked")
	public T resolve(HGHandle key)
    {
        return (T)node.get(key);
    }
}