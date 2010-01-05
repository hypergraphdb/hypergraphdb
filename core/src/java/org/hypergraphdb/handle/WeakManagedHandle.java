/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
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
