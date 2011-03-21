/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.util.Pair;

public class RefDelegateStorageGraph implements StorageGraph
{
    private Map<HGPersistentHandle, HGPersistentHandle> delegates;
    private StorageGraph wrapped;
    
    public RefDelegateStorageGraph(StorageGraph wrapped, 
                                   Map<HGPersistentHandle, HGPersistentHandle> delegates)
    {
        this.wrapped = wrapped;
        this.delegates = delegates;
    }
    
    public byte[] getData(HGPersistentHandle handle)
    {
        HGPersistentHandle del = delegates.get(handle);
        return del != null ? wrapped.getData(del) : wrapped.getData(handle);
    }

    
    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        HGPersistentHandle del = delegates.get(handle);
        return del != null ? wrapped.getLink(del) : wrapped.getLink(handle);
    }

    public HGPersistentHandle store(HGPersistentHandle handle,
                                    HGPersistentHandle[] link)
    {
        return wrapped.store(handle, link);
    }

    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        return wrapped.store(handle, data);
    }

    public Set<HGPersistentHandle> getRoots()
    {
        return wrapped.getRoots();
    }

    public Iterator<Pair<HGPersistentHandle, Object>> iterator()
    {
        return wrapped.iterator();
    }
}
