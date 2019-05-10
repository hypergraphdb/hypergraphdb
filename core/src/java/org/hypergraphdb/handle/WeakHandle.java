/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;

public class WeakHandle extends WeakReference<Object> implements HGLiveHandle, Comparable<HGHandle>
{
    public static ThreadLocal<Boolean> returnEnqueued = new ThreadLocal<Boolean>();
    
    private HGPersistentHandle persistentHandle;
    private byte flags;

    public WeakHandle(Object ref,
                      HGPersistentHandle persistentHandle,
                      byte flags,
                      ReferenceQueue<Object> refQueue)
    {
        super(ref, refQueue);
        this.persistentHandle = persistentHandle;
        this.flags = flags;
    }
   
    public byte getFlags()
    {
        return flags;
    }

    public HGPersistentHandle getPersistent()
    {
        return persistentHandle;
    }

    public Object getRef()
    {
        //
        // Here, we want to return null when the object is about to be garbage
        // collected. This will be indicated by the fact that the object will
        // be enqueued in the ReferenceQueue for this WeakReference.
        //

        // Obviously, we are doing something not orthodox here. The main danger
        // and the first thing to examine in case of weird behavior with this
        // is a situation where an atom gets removed from the 'atoms' WeakHashMap
        // in the cache (hence it's being garbage collected) and 'getRef' is called
        // after that but before the reference gets enqueued. Is that possible at all?
        // It looks like the GC manages references and references queues in a high-priority
        // thread. Also, when an object is going to be garbage collect its references are
        // added to a "pending" list from where they are further enqueued. The 'isEnqueued'
        // method actually checks for "pending or already enqueued" so we should be fine.

        //
        // However, there is a special case in which we don't want to return null: when
        // we are in the weak reference queue cleanup thread. Because cleanup may involve
        // a call to 'getRef', this may result in a deadlock within the ref
        // cleanup thread itself. For this we use the thread local variable 'returnEnqueued'
        //
        Object x = get();      
        if (isEnqueued())
        {
            Boolean f = returnEnqueued.get();
            if (f != null && f.booleanValue())
                return x;
            x = null;
            do
            {
                try { synchronized (this) { wait(100); } } catch (InterruptedException ex) { }
            } while (isEnqueued());
        }
        return x;
    }
    
    public void accessed() { }
    
    public final int hashCode()
    {
        return persistentHandle.hashCode();
    }

    public final boolean equals(Object other)
    {
        if (other == null || ! (other instanceof HGHandle))
            return false;
    		return persistentHandle.equals(((HGHandle)other).getPersistent());
    }

    public String toString()
    {
        return persistentHandle.toString();
    }

    public int compareTo(HGHandle h)
    {
        if (h instanceof HGPersistentHandle)
            return this.persistentHandle.compareTo((HGPersistentHandle)h);
        else
            return this.persistentHandle.compareTo(((HGLiveHandle)h).getPersistent());
    }
}
