/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.HashSet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * A {@link StorageGraph} bound to a {@link HGStore}. It's based on a set of root handles
 * and its iterator will traverse the primitive storage graph starting from those roots. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGStoreSubgraph implements StorageGraph
{
    private Set<HGPersistentHandle> roots;
    private HGStore store;
    
    /**
     * <p>
     * Construct a new {@link HGStore} based {@link StorageGraph} 
     * </p>
     * @param root A single root, starting point of the storage graph.
     * @param store The backing store instance.
     */
    public HGStoreSubgraph(HGHandle root, HGStore store)
    {
        this.roots = new HashSet<HGPersistentHandle>();
        this.roots.add(root.getPersistent());
        this.store = store;
    }
    
    /**
     * <p>
     * Construct a new {@link HGStore} based {@link StorageGraph}
     * with multiple roots. 
     * </p>
     * @param root A single root, starting point of the storage graph.
     * @param store The backing store instance.
     */
    public HGStoreSubgraph(Set<HGHandle> roots, HGStore store)
    {
        this.roots = new HashSet<HGPersistentHandle>();
        for (HGHandle root : roots)
            this.roots.add(root.getPersistent());
        this.store = store;
    }

    
    public byte[] getData(HGPersistentHandle handle)
    {
        return store.getData(handle);
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        return store.getLink(handle);
    }

    public HGPersistentHandle store(HGPersistentHandle handle,
                                    HGPersistentHandle[] link)
    {
        return store.store(handle, link);
    }

    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        return store.store(handle, data);
    }

    public Set<HGPersistentHandle> getRoots()
    {
        return roots;
    }

    public Iterator<Pair<HGPersistentHandle, Object>> iterator()
    {
        return new SubgraphIterator();
    }

    private class SubgraphIterator implements Iterator<Pair<HGPersistentHandle, Object>>
    {
        LinkedList<HGPersistentHandle> remaining = new LinkedList<HGPersistentHandle>();
        HashSet<HGPersistentHandle> visited = new HashSet<HGPersistentHandle>();
        
        public SubgraphIterator()
        {
            for (HGPersistentHandle root : roots)
                remaining.addLast(root);
            
            //TODO some UUIDs should not be visited?
            visited.add(store.getTransactionManager().getHyperGraph().getHandleFactory().nullHandle());
        }
        
        public boolean hasNext()
        {
            return !remaining.isEmpty();
        }

        public Pair<HGPersistentHandle, Object> next()
        {
            Pair<HGPersistentHandle, Object> result = null;
            HGPersistentHandle h = remaining.removeFirst();
            HGPersistentHandle[] link = store.getLink(h);
           
            if (link == null)
            {
                byte [] data = store.getData(h);
                //TODO throw exception for missing data
                if (data != null) 
                {
                    visited.add(h);
                    result = new Pair<HGPersistentHandle, Object>(h, data);
                }
                else
                	System.err.println("oops, handle not in store " + h);
           }
           else
           {
               visited.add(h);
               for (HGPersistentHandle x : link)
               {                   
                   // do we want to prevent null from being returned...it's legit for some 
                   if (!visited.contains(x) /* && (store.getData(x) != null || store.getLink(x) != null) */ ) 
                       remaining.addLast(x);
               }

               result = new Pair<HGPersistentHandle, Object>(h, link);
           }

           return result;
       }

       public void remove()
       {
           throw new UnsupportedOperationException();
       }
    }    
}