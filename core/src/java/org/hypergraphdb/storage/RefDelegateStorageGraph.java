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

    
    public Set<HGPersistentHandle> getRoots()
    {
        return wrapped.getRoots();
    }

    public Iterator<Pair<HGPersistentHandle, Object>> iterator()
    {
        return wrapped.iterator();
    }
}