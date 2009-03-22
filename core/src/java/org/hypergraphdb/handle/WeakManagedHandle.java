package org.hypergraphdb.handle;

import java.lang.ref.ReferenceQueue;

import org.hypergraphdb.HGPersistentHandle;

public class WeakManagedHandle extends WeakHandle implements HGManagedLiveHandle 
{
    private long retrievalCount;
    private long lastAccessTime;
    
    public WeakManagedHandle(Object ref, 
                                HGPersistentHandle persistentHandle, 
                                byte flags, 
                                ReferenceQueue<Object> refQueue,
                                long retrievalCount,
                                long lastAccessTime)
    {
        super(ref, persistentHandle, flags, refQueue);
        this.retrievalCount = retrievalCount;
        this.lastAccessTime = lastAccessTime;
    }
    
    public void accessed() 
    {
        lastAccessTime = System.currentTimeMillis();
        retrievalCount++;
    }

    public long getLastAccessTime() 
    {
        return lastAccessTime;
    }

    public long getRetrievalCount() 
    {
        return retrievalCount;
    }
}