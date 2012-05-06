/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import java.lang.ref.ReferenceQueue;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HGAtomAttrib;
import org.hypergraphdb.HGAtomCache;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSystemFlags;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.event.HGAtomEvictEvent;
import org.hypergraphdb.handle.DefaultManagedLiveHandle;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.handle.PhantomHandle;
import org.hypergraphdb.handle.PhantomManagedHandle;
import org.hypergraphdb.util.CloseMe;
import org.hypergraphdb.util.WeakIdentityHashMap;

public class PhantomRefAtomCache implements HGAtomCache 
{   
    private HyperGraph graph = null;
    
    private HGCache<HGPersistentHandle, IncidenceSet> incidenceCache = null; // to be configured by the HyperGraph instance
    
    private final Map<HGPersistentHandle, PhantomHandle> 
        liveHandles = new HashMap<HGPersistentHandle, PhantomHandle>();
        
    private Map<Object, HGLiveHandle> atoms = 
        new WeakIdentityHashMap<Object, HGLiveHandle>();
    
    private Map<HGLiveHandle, Object> frozenAtoms = 
        new IdentityHashMap<HGLiveHandle, Object>();
    private ColdAtoms coldAtoms = new ColdAtoms();
    
    public static final long DEFAULT_PHANTOM_QUEUE_POLL_INTERVAL = 500;
    
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    @SuppressWarnings("unchecked")
    private ReferenceQueue refQueue = new ReferenceQueue();
    private PhantomCleanup phantomCleanupThread = new PhantomCleanup();
    private long phantomQueuePollInterval = DEFAULT_PHANTOM_QUEUE_POLL_INTERVAL;
    private boolean closing = false;
    
    //
    // This handle class is used to read atoms during closing of the cache. Because
    // closing the HyperGraph may involve a lot of cleanup activity where it's necessary
    // to read atoms (mostly types) into main memory just temporarily, we need some
    // way to enact this temporarility. 
    //
    private static class TempLiveHandle extends DefaultManagedLiveHandle
    {
        public TempLiveHandle(Object ref, HGPersistentHandle persistentHandle, byte flags)
        {
            super(ref, persistentHandle, flags, 0, 0);
        }
        
        public void setRef(Object ref)
        {
            this.ref = ref;
        }
    }   
    
    private void processRefQueue() throws InterruptedException
    {
        PhantomHandle ref = (PhantomHandle)refQueue.remove(phantomQueuePollInterval);
        while (ref != null)
        {
            graph.getEventManager().dispatch(graph, 
                     new HGAtomEvictEvent(ref, ref.fetchRef()));
            lock.writeLock().lock();
            try
            {
                liveHandles.remove(ref.getPersistent());
            }
            finally
            {
                lock.writeLock().unlock();
            }
            ref.clear();
            synchronized (ref) { ref.notifyAll(); }         
            ref = (PhantomHandle)refQueue.poll();
        }
    }
    
    private class PhantomCleanup extends Thread 
    {
        private boolean done;
        
        public void run() 
        {
            PhantomHandle.returnEnqueued.set(Boolean.TRUE);
            for (done = false; !done; ) 
            {
                try 
                {
                    processRefQueue();
                } 
                catch (InterruptedException exc) 
                {
                    Thread.currentThread().interrupt();
                }
                catch (Throwable t)
                {
                    System.err.println("PhantomCleanup thread caught an unexpected exception, stack trace follows:");
                    t.printStackTrace(System.err);
                }
            }
            PhantomHandle.returnEnqueued.set(Boolean.FALSE);
        }

        public void end()
        {
            this.done = true;
        }
    }
    
    public PhantomRefAtomCache()
    {
        phantomCleanupThread.setPriority(Thread.MAX_PRIORITY);
        phantomCleanupThread.setDaemon(true);       
        phantomCleanupThread.start();
    }
    
    public void setIncidenceCache(HGCache<HGPersistentHandle, IncidenceSet> cache)
    {
        this.incidenceCache= cache;
    }
    
    public HGCache<HGPersistentHandle, IncidenceSet> getIncidenceCache()
    {
        return incidenceCache;
    }
    
    public void setHyperGraph(HyperGraph hg) 
    {
        this.graph = hg;
        phantomCleanupThread.setName("HGCACHE Cleanup - " + graph.getLocation());
    }
    
    public HGLiveHandle atomAdded(HGPersistentHandle pHandle, Object atom, final HGAtomAttrib attrib) 
    {
        return atomRead(pHandle, atom, attrib);
    }
    
    @SuppressWarnings("unchecked")
    public HGLiveHandle atomRead(HGPersistentHandle pHandle, 
                                 Object atom,
                                 final HGAtomAttrib attrib) 
    {
        if (closing)
        {
            HGLiveHandle result = new TempLiveHandle(atom, pHandle, attrib.getFlags());
            atoms.put(atom, result);
            return result;
        }       
        lock.writeLock().lock();
        PhantomHandle h = null;
        try
        {           
            h = liveHandles.get(pHandle);
            if (h != null)
                return h;
            if ( (attrib.getFlags() & HGSystemFlags.MANAGED) == 0)
                h = new PhantomHandle(atom, pHandle, attrib.getFlags(), refQueue);
            else
                h = new PhantomManagedHandle(atom, 
                        pHandle, 
                        attrib.getFlags(), 
                        refQueue,
                        attrib.getRetrievalCount(),
                        attrib.getLastAccessTime());         
            atoms.put(atom, h);
            liveHandles.put(pHandle, h);
            coldAtoms.add(atom);
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return h;
    }


    public HGLiveHandle atomRefresh(HGLiveHandle handle, Object atom, boolean replace) 
    {
        if (closing)
        {
            if (handle instanceof PhantomHandle)
                ((PhantomHandle)handle).storeRef(atom);
            else
                ((TempLiveHandle)handle).setRef(atom);
            return handle;
        }
        if (handle == null)
            throw new NullPointerException("atomRefresh: handle is null.");
        
        lock.writeLock().lock();
        
        try
        {
            PhantomHandle ph = (PhantomHandle)handle;
            PhantomHandle existing = liveHandles.get(ph.getPersistent());
            
            if (existing != ph)
            {
                if (existing != null)
                {
                    liveHandles.remove(existing.getPersistent());
                    atoms.remove(existing.getRef());                
                }
                ph.storeRef(atom);
                liveHandles.put(ph.getPersistent(), ph);
                atoms.put(atom, ph);
                coldAtoms.add(atom);
            }       
            else if (ph.getRef() != atom)
            {
                atoms.remove(ph.getRef());
                ph.storeRef(atom);
                atoms.put(atom, ph);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return handle;
    }

    public void close() 
    {   
        closing = true;
        phantomCleanupThread.end(); 
        while (phantomCleanupThread.isAlive() )
            try { phantomCleanupThread.join(); } catch (InterruptedException ex) { }
        PhantomHandle.returnEnqueued.set(Boolean.TRUE);
        try { processRefQueue(); } catch (InterruptedException ex) { }
        for (Iterator<Map.Entry<HGPersistentHandle, PhantomHandle>> i = liveHandles.entrySet().iterator(); 
             i.hasNext(); ) 
        {
            PhantomHandle h = i.next().getValue();
            Object x = h.fetchRef();
            graph.getEventManager().dispatch(graph, new HGAtomEvictEvent(h, x));            
            if (h.isEnqueued())
            {
                h.clear();
            }
        }
        PhantomHandle.returnEnqueued.set(Boolean.FALSE);
        frozenAtoms.clear();        
        incidenceCache.clear();
        if (incidenceCache instanceof CloseMe)
            ((CloseMe)incidenceCache).close();
        atoms.clear();
        liveHandles.clear();
    }

    public HGLiveHandle get(HGPersistentHandle pHandle) 
    {
        lock.readLock().lock();
        try
        {
            PhantomHandle h = liveHandles.get(pHandle);
            if (h != null)
                h.accessed();
            return h;
        }
        finally
        {
            lock.readLock().unlock();           
        }
    }

    public HGLiveHandle get(Object atom) 
    {
        lock.readLock().lock();
        try
        {
            return atoms.get(atom);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public void remove(HGHandle handle) 
    {
        lock.writeLock().lock();
        try
        {
      		HGLiveHandle lhdl = null;
      		
      		if (handle instanceof HGLiveHandle)
      			lhdl = (HGLiveHandle)handle;
      		else 
      			lhdl = get(handle.getPersistent());
      		
      		if (lhdl != null)
      		{
      			atoms.remove(lhdl.getRef());
            // Shouldn't use clear here, since we might be gc-ing the ref!
            if (lhdl instanceof PhantomHandle)
                ((PhantomHandle)lhdl).storeRef(null);
      			liveHandles.remove(lhdl.getPersistent());
      		}
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    
    public boolean isFrozen(HGLiveHandle handle) 
    {
        synchronized (frozenAtoms)
        {
            return frozenAtoms.containsKey(handle);
        }
    }

    public void freeze(HGLiveHandle handle) 
    {
        Object atom = handle.getRef();
        if (atom != null)
            synchronized (frozenAtoms)
            {
                frozenAtoms.put(handle, atom);
            }
    }   

    public void unfreeze(HGLiveHandle handle) 
    {
        synchronized (frozenAtoms)
        {       
            frozenAtoms.remove(handle);
        }
    }
}
