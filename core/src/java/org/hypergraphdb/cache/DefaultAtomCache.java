/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.cache;

import java.util.HashMap;

import java.util.Iterator;

import org.hypergraphdb.HGAtomAttrib;
import org.hypergraphdb.HGAtomCache;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSystemFlags;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.handle.DefaultManagedLiveHandle;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.util.ActionQueueThread;
import org.hypergraphdb.event.HGAtomEvictEvent;

/**
 * <p>
 * A default, simple implementation of a run-time cache of hypergraph atoms. 
 * </p>
 * 
 * <p>
 * <strong>IMPLEMENTATION NOTE:</strong> This implementation maintains usage statistics 
 * (access count and last time of access), calculates an importance function based on
 * those statistics and maintains a priority queue based on that importance. This incurs
 * a significant memory overhead on a per atom basis. But the importance-based eviction
 * policy is quite accurate. This implementation makes more sense when atoms are relatively
 * large, but we need to come up and experiment with other schemas for cache maintenance. 
 * Something based on Java weak reference might work well, for example.
 * </p>
 *
 * TODO: this implementation is NOT thread-safe. Mutexes need to be inserted at various
 * cache manipulation points. 
 * 
 * @author Borislav Iordanov
 */
public final class DefaultAtomCache implements HGAtomCache
{
    private static class LiveHandle extends DefaultManagedLiveHandle
    {
    	LiveHandle next, prev;
    	public LiveHandle(Object ref, 
    					  HGPersistentHandle pHandle, 
    					  byte flags)
    	{
    		super(ref, pHandle, flags, 1L, System.currentTimeMillis());
    	}    	
    	public LiveHandle(Object ref, 
    					  HGPersistentHandle pHandle, 
    					  byte flags, 
    					  long retrievalCount,
    					  long lastAccessTime)
    	{
    		super(ref, pHandle, flags, retrievalCount, System.currentTimeMillis());
    	}
    	    	
    	void setRef(Object ref)
    	{
    		this.ref = ref;
    	}
    }
	    
	private HyperGraph hg;
    
    /**
     * HGPersistentHandle -> LiveHandle
     */
    private final HashMap<HGPersistentHandle, LiveHandle> 
    	liveHandles = new HashMap<HGPersistentHandle, LiveHandle>();
    
    /**
     * Object -> LiveHandle
     */
    private final HashMap<Object, LiveHandle> 
    	atoms = new HashMap<Object, LiveHandle>();
    
    /**
     * HGPersistentHandle -> incidence set  
     */
    private HGCache<HGPersistentHandle, IncidenceSet> incidenceSets = null;
    
    private long retrievalCount = 0;
    private long lastAccessTime = System.currentTimeMillis();
    private double retrievalFrequencyWeight = 10.0;
    private double lastAccessTimeWeight = 1.0;
    private LiveHandle atomQueueTail = null;
    private ActionQueueThread queueThread = null;
    
    //
    // Configuration parameters.
    //
    private long maxAtoms = 100;
    private long maxIncidenceSets = 10;
    
	public double importanceOf(LiveHandle cached)
	{			
		return retrievalFrequencyWeight*((double)cached.getRetrievalCount() / (double)retrievalCount) + 
			   lastAccessTimeWeight*((double)cached.getLastAccessTime() / (double)lastAccessTime);
	}
	
	private void importanceUp(LiveHandle cached)
	{
		double importance = importanceOf(cached);
		while (cached.next != null &&
			   importance > importanceOf(cached.next))
		{
			//
			// This looks a bit incomprehensible: draw 4 boxes in line 
			// with next and prev pointers and go through the exercise
			// of rearranging the pointers so that the second box should
			// move before the fourth. That's moving an element up the queue.
			//
			// If it is common that elements suddenly jump a lot in importance
			// so that this iteration is repeated more than a couple of times, 
			// perhaps a more efficient loop where the destination is first
			// determined would be better. An insertion of a new element into
			// the cache is one such case, but presumably insertions are rare
			// compared to access.
			//
			cached.next.prev = cached.prev;
			if (cached.prev != null)
				cached.prev.next = cached.next;
			cached.prev = cached.next;
			if (cached.next.next != null)
				cached.next.next.prev = cached;								
			cached.next = cached.next.next;
			cached.prev.next = cached;
			if (atomQueueTail == cached)
				atomQueueTail = cached.prev;
		}
	}    

	private void insert(LiveHandle handle)
	{
    	//
    	// Always add a newly read atom to the cache. But free up some space if we've 
    	// used it all. 
    	//
    	if (liveHandles.size() >= maxAtoms)
    	{
    		//
    		// We must pay attention not to generate to many evict actions and
        	// give the chance to eviction to actually occur. So we block until all scheduled
        	// queue maintanance actions have been completed.
    		//
    		queueThread.addAction(new AtomsEvictAction(liveHandles.size() / 10));
    		queueThread.setPriority(Thread.NORM_PRIORITY + 2);
//    		queueThread.completeAll();
    	}
        liveHandles.put(handle.getPersistent(), handle);
        atoms.put(handle.getRef(), handle);
        queueThread.addAction(new AddAtomAction(handle));		
	}
	
	public DefaultAtomCache()
	{
		queueThread = CacheActionQueueSingleton.get();
	}
	
	public void setIncidenceCache(HGCache<HGPersistentHandle, IncidenceSet> cache)
	{
		this.incidenceSets = cache;
	}
	
	public HGCache<HGPersistentHandle, IncidenceSet> getIncidenceCache()
	{
		return incidenceSets;
	}

	public void setMaxAtoms(long maxAtoms)
	{
		this.maxAtoms = maxAtoms;
	}
	
	public long getMaxAtoms()
	{
		return maxAtoms;
	}
	
	public void setMaxIncidenceSets(long maxIncidenceSets)
	{
		this.maxIncidenceSets = maxIncidenceSets;
	}
	
	public long getMaxIncidenceSets()
	{
		return maxIncidenceSets;
	}
	
    public void setHyperGraph(HyperGraph hg)
    {
    	this.hg = hg;
    }
	        
	public void close()
	{
    	for (Iterator<LiveHandle> i = liveHandles.values().iterator(); i.hasNext(); )
    	{
    		HGLiveHandle lHandle = (HGLiveHandle)i.next();
    		hg.getEventManager().dispatch(hg, new HGAtomEvictEvent(lHandle, lHandle.getRef()));    		
    	}		
    	liveHandles.clear();
    	atoms.clear();
    	incidenceSets.clear();
    	atomQueueTail = null;		
    	queueThread.stopRunning();
	}
	
    /**
     * <p>Lookup in the cache for a live handle corresponding to a persistent
     * handle.</p> 
     */
    public HGLiveHandle get(final HGPersistentHandle pHandle)
    {
    	LiveHandle result = liveHandles.get(pHandle);
    	if (result == null)
    		return null;
    	result.accessed();
    	retrievalCount++;
    	lastAccessTime = System.currentTimeMillis();
    	queueThread.addAction(new AtomAccessedAction(result));
    	return result;
    }
    
    /**
     * <p>Retrieve the live handle of an atom instance.</p>
     */
    public HGLiveHandle get(final Object atom)
    {
    	LiveHandle result = atoms.get(atom);
    	if (result == null)
    		return null;
    	result.accessed();
    	retrievalCount++;
    	lastAccessTime = System.currentTimeMillis();
    	queueThread.addAction(new AtomAccessedAction(result));
    	return result;
    }
    
    public HGLiveHandle atomAdded(final HGPersistentHandle pHandle, final Object atom, final HGAtomAttrib attrib)
    {
        return atomRead(pHandle, atom, attrib);
    }
    
    /**
     * <p>Associate an atom instance and a persistent handle with a live handle.</p> 
     */
    public HGLiveHandle atomRead(final HGPersistentHandle pHandle, final Object atom, final HGAtomAttrib attrib)
    {
        LiveHandle lHandle = null;
        if ( (attrib.getFlags() & HGSystemFlags.MANAGED) != 0)
            lHandle = new LiveHandle(atom, pHandle, attrib.getFlags(), attrib.getRetrievalCount(), attrib.getLastAccessTime());
        else
            lHandle = new LiveHandle(atom, pHandle, attrib.getFlags());
    	insert(lHandle);
        return lHandle;
    }

    public HGLiveHandle atomRefresh(HGLiveHandle handle, Object atom, boolean replace)
    {
    	LiveHandle existing = liveHandles.get(handle.getPersistent());  	
    	if (existing != null)
    	{
    		atoms.remove(existing.getRef());    		
    		existing.setRef(atom);
    		atoms.put(atom, existing);		    		
    	}
    	else
    	{
        	LiveHandle lHandle = (LiveHandle)handle;    		
    		lHandle.setRef(atom);    		
    		insert(lHandle);    		
    	}
    	return handle;
    }
    
    public void freeze(HGLiveHandle handle)
    {
    	queueThread.addAction(new AtomDetachAction((LiveHandle)handle));
    }
    
    public void unfreeze(HGLiveHandle handle)
    {
    	queueThread.addAction(new AddAtomAction((LiveHandle)handle));    	
    }
    
    public boolean isFrozen(HGLiveHandle handle)
    {
    	LiveHandle h = (LiveHandle)handle;
    	return h.prev == null && h.next == null;
    }
    

    /**
     * <p>Remove a live handle and all its associations from the cache.</p>
     */
    public void remove(HGHandle handle)
    {
			HGLiveHandle lhdl = null;
			
  		if (handle instanceof HGLiveHandle)
  			lhdl = (HGLiveHandle)handle;
  		else 
  			lhdl = get(handle.getPersistent());
    	
  		if (lhdl != null)
  		{
	    	incidenceSets.remove(lhdl.getPersistent());
	    	atoms.remove(lhdl.getRef());
	    	liveHandles.remove(lhdl.getPersistent());
	    	queueThread.addAction(new AtomDetachAction((LiveHandle)lhdl));        
	    	((LiveHandle)lhdl).setRef(null);
  		}
    }
    
    //
    // Actions for queue maintenance
    //
    private class AtomAccessedAction implements Runnable
    {
    	LiveHandle atom;
    	AtomAccessedAction(LiveHandle atom) { this.atom = atom; }
    	public void run()
    	{    		
    		importanceUp(atom);
    	}
    }
    
    private class AddAtomAction implements Runnable
    {
    	LiveHandle atom;
    	AddAtomAction(LiveHandle atom) { this.atom = atom; }
    	public void run()
    	{
    		if (atomQueueTail == null)
    			atomQueueTail = atom;
    		else
    		{
				atom.next = atomQueueTail;
				atomQueueTail.prev = atom;
				atomQueueTail = atom;
				importanceUp(atom);
    		}
    	}
    }

    private class AtomDetachAction implements Runnable
    {
    	LiveHandle atom;
    	public AtomDetachAction(LiveHandle atom) { this.atom = atom; }
    	public void run()
    	{
    		if (atom.prev != null)
    			atom.prev.next = atom.next;
    		if (atom.next != null)
    			atom.next.prev = atom.prev;
    		atom.prev = atom.next = null;
    	}
    }
    
    private class AtomsEvictAction implements Runnable
    {
    	long n;
    	AtomsEvictAction(long n) { this.n = n; }
    	public void run()
    	{
    		if (atomQueueTail == null)
    			return;
    		LiveHandle newTail = atomQueueTail;    		
    		while (n-- > 0 && newTail.next != null)
    		{
    			LiveHandle current = newTail;
    			liveHandles.remove(newTail.getPersistent());
    			atoms.remove(newTail.getRef());
    			hg.getEventManager().dispatch(hg, new HGAtomEvictEvent(newTail, newTail.getRef()));    			
    			newTail.setRef(null);
    			newTail =  newTail.next;
    			current.prev = current.next = null;
    		}
    		if (newTail.next == null)
    		{
    			liveHandles.remove(newTail.getPersistent());
    			atoms.remove(newTail.getRef());
    			hg.getEventManager().dispatch(hg, new HGAtomEvictEvent(newTail, newTail.getRef()));    			
    			newTail.setRef(null);
    			newTail.prev = newTail.next = null;
    			atomQueueTail = null;
    		}
    		else
    		{
    			newTail.prev = null;
    			atomQueueTail = newTail;
    		}    		
    	}
    }
}
