/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * A {@link StorageGraph} bound to a RAM map to be populated explicitly 
 * before use.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class RAMStorageGraph implements StorageGraph
{
    private Set<HGPersistentHandle> roots;
    private Map<HGPersistentHandle, Object> map = new HashMap<HGPersistentHandle, Object>();
    
    public RAMStorageGraph()
    {
        this.roots = new HashSet<HGPersistentHandle>();
    }
    
    public RAMStorageGraph(HGPersistentHandle root)
    {
        this.roots = new HashSet<HGPersistentHandle>();
        this.roots.add(root);
    }
    
    public RAMStorageGraph(Set<HGPersistentHandle> roots)
    {
        this.roots = roots;
    }
    
    public void translateHandles(Map<HGHandle, HGHandle> subst)
    {
        Map<HGPersistentHandle, Object> translated = new HashMap<HGPersistentHandle, Object>();
        for (Map.Entry<HGPersistentHandle, Object> e : map.entrySet())
        {
            if (e.getValue() instanceof HGPersistentHandle[])
            {
                HGPersistentHandle[] A = (HGPersistentHandle[])e.getValue();
                for (int i = 0; i < A.length; i++)
                {
                    HGHandle h = subst.get(A[i]);
                    if (h != null)
                        A[i] = h.getPersistent();
                }
            }
            HGHandle h = subst.get(e.getKey());            
            if (h == null)            	
            	h = e.getKey();
            translated.put(h.getPersistent(), e.getValue());
        }
        map = translated;
    }
    
    public void put(HGPersistentHandle handle, HGPersistentHandle [] linkData)
    {
        map.put(handle, linkData);
    }
    
    public void put(HGPersistentHandle handle, byte [] data)
    {
        map.put(handle, data);
    }
    
    public HGPersistentHandle store(HGPersistentHandle handle,
                                    HGPersistentHandle[] link)
    {
        put(handle, link);
        return handle;
    }

    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        put(handle, data);
        return handle;
    }

    public byte[] getData(HGPersistentHandle handle)
    {
        return (byte[])map.get(handle);
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        return (HGPersistentHandle[])map.get(handle);
    }

    public Set<HGPersistentHandle> getRoots()
    {
        return roots;
    }

    public Iterator<Pair<HGPersistentHandle, Object>> iterator()
    {
        return new Iterator<Pair<HGPersistentHandle, Object>>()
        {
            final Iterator<Map.Entry<HGPersistentHandle, Object>> i = map.entrySet().iterator();

            public boolean hasNext()
            {
                return i.hasNext();
            }

            public Pair<HGPersistentHandle, Object> next()
            {
                Map.Entry<HGPersistentHandle, Object> e = i.next();
                return new Pair<HGPersistentHandle, Object>(e.getKey(), e.getValue());
            }

            public void remove()
            {
                i.remove();
            }            
        };
    }
}
