/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.cache.HGCache;
import org.hypergraphdb.handle.HGLiveHandle;

/**
 * <p>The <code>HGAtomCache</code> interface abstracts the HyperGraph
 * caching activities in order for different caching policies and implementations
 * to be configured and plugged. A successful caching policy will largely depend 
 * on a particular application. For instance, some applications may maintain most HyperGraph
 * atoms within their own data structures whilst others would entirely rely on HyperGraph
 * and continuously query for atoms on a need by need basis. 
 * </p>
 * 
 * <p>
 * The cache is reminiscent to a memory manager. Both HyperGraph and its
 * HGTypeSystem rely on the cache to manage live vs. persistent information.
 * </p>
 * 
 * <p>
 * This interface is made public in order to allow for custom plugging of atom caching
 * policies. It is <strong>never</strong> to be used directly be applications.
 * </p>
 * 
 * <p>
 * The <code>HGAtomCache</code> is responsible for the following:
 * 
 * <ul>
 * <li>Manage run-time instances of atoms.</li>
 * <li>Create live handle for newly loaded atoms.</li>
 * <li>Generate an <code>HGAtomEvictedEvent</code> for every atom that is removed 
 * from the cache.</li>
 * <li>Cache and manages incidence sets on a per-atom basis.</li>
 * <li>Provide mappings between atoms' live handles and their persistence handles.</li> 
 * </ul>
 * </p>
 */
public interface HGAtomCache 
{
	/**
	 * <p>
	 * Set the <code>HyperGraph</code> within which this cache is operating. A given
	 * cache implementation may or may not need a reference to the <code>HyperGraph</code>
	 * instance, depending on the level of sophistication of the cache. For example,
	 * some cache policies may want to persist certain usage statistics in the 
	 * <code>HGStore</code>, others may want to attach particular importance to certain
	 * atom types etc.
	 * </p>
	 *  
	 * @param hg The <code>HyperGraph</code> instance.
	 */
	void setHyperGraph(final HyperGraph hg);
	
    /**
     * <p>
     * Inform the cache that a new atom has just been added to the database.
     * The cache is free to decide whether that atom should be actually cached or not,
     * but in any case it must construct and return <code>HGLiveHandle</code> for
     * the atom.
     * </p>
     * 
     * @param pHandle The persistent handle of the atom.
     * @param atom The run-time instance of the atom.
     * @param attrib Atom management related attributes that must be stored as part of its live handle.
     * @return A valid <code>HGLiveHandle</code> to the atom.
     */	
	HGLiveHandle atomAdded(final HGPersistentHandle pHandle, final Object atom, final HGAtomAttrib attrib);
	
	/**
	 * <p>
	 * Inform the cache that an atom has just been read from persistent storage.
	 * The cache is free to decide whether that atom should be actually cached or not,
	 * but in any case it must construct and return <code>HGLiveHandle</code> for
	 * the atom.
	 * </p>
	 * 
	 * <p>
	 * If the atom corresponding to specified persistent handle is already in the cache, 
	 * it should be replaced with the new passed in value.
	 * </p>
	 * 
	 * @param pHandle The persistent handle of the atom.
	 * @param atom The run-time instance of the atom.
	 * @param attrib Atom management related attributes that must be stored as part of its live handle.
	 * @return A valid <code>HGLiveHandle</code> to the atom.
	 */
	HGLiveHandle atomRead(final HGPersistentHandle pHandle, final Object atom, final HGAtomAttrib attrib);

	/**
	 * <p>
	 * Inform the cache that an atom with system-level attributes has been loaded.
	 * This has the same function as the simpler version of <code>atomRead</code>, but
	 * the cache must construct an instance of <code>HGExtLiveHandle</code> for purposes
	 * of <em>non-default</em> atom management.
	 * </p>
	 * 
	 * @param pHandle The persistent handle of the atom.
	 * @param atom The run-time instance of the atom.
	 * @param flags The system flags for this atom.
	 * @param retrievalCount The recorded overall retrieval count for this atom.
	 * @param lastAccessTime A timestamps (in milliseconds) representing the last
	 * time this atom was accessed.
	 * @return A <code>HGManagedLiveHandle</code> encapsulating the atom instance,
	 * persistence handle and system-level attributes.
	 */
//	HGManagedLiveHandle atomRead(final HGPersistentHandle pHandle, 
//								 final Object atom, 
//								 final byte flags, 
//								 final long retrievalCount, 
//								 final long lastAccessTime);
	/**
	 * <p>Replace the runtime instance of an atom with a new value. This method is invoked when <code>HyperGraph</code>
	 * needs to inform the cache about a change of the value of an atom. The cache must 
	 * assign the new reference to the cached live handle and, as the case may be, reinsert the atom
	 * into its caching structures. 
	 * </p>
	 * 
	 * <p>
	 * Note that there are two important cases of "refreshing" an atom in the cache - the atom
	 * being reloaded from permanent storage or when there is an actual value change. In the 
	 * former case, a transaction abort does not need to roll back changes in the caching
	 * structures while in the latter it does! The two cases are distinguished by the third
	 * parameter of this method. ??? Note that the cache should perform a replace if <code>handle.getRef() != null
	 * && handle.getRef() != atom</code> because this signals that a new value must be stored.
     * If the atom's value hasn't changed (i.e. if <code>handle.getRef() == atom</code>) then
     * nothing is done.
	 * </p>
	 * 
	 * @param handle The <code>HGLiveHandle</code> handle of the atom to be refreshed.
	 * @param atom The atom value. <code>HyperGraph</code> will obtain this value
	 * either from the cache, in case the atom has already been re-fetched from storage
	 * after the eviction event, or it will retrieve and create a new run-time instance.
	 * @param replace <code>true</code> if this is a new atom value (old must be restored
	 * if the transaction aborts) and <code>false</code> if this is simply a reload from
	 * permanent storage where the effects of that reload may (or may not, depending on the
	 * implementation) be reversed in case of a transaction abort.
	 * @return Possibly a new live handle instance or the <code>handle</code> parameter depending
	 * on the implementation. If a new instance is return, the reference of the old instance
	 * is cleared.
	 */
	HGLiveHandle atomRefresh(HGLiveHandle handle, Object atom, boolean replace);
	
	/**
	 * <p>
	 * Retrieve an atom from the cache by its persistent handle. 
	 * </p>
	 * 
	 * @param pHandle The <code>HGPersistentHandle</code> of the desired atom.
	 * @return The <code>HGLiveHandle</code> of the atom or <code>null</code> if
	 * the atom is not in the cache.
	 */
	HGLiveHandle get(final HGPersistentHandle pHandle);
	
	/**
	 * <p>
	 * Retrieve the <code>HGLiveHandle</code> of a run-time atom instance.
	 * </p>
	 * 
	 * @param atom The atom object.
	 * @return The <code>HGLiveHandle</code> of that atom or <code>null</code> if 
	 * the atom is not in the cache.
	 */
	HGLiveHandle get(final Object atom);
	
	/**
	 * <p>Force a removal a given atom from the cache. This method is generally invoked when 
	 * the atom is actually deleted from the graph.</p>
	 * 
	 * <p>
	 * If the incidence set of this atom was loaded in the cache, it should be removed as well.
	 * </p>
	 * 
	 * @param handle The <code>HGLiveHandle</code> of the atom to be removed. If the atom is
	 * currently not in the cache, nothing should be done.
	 */
	void remove(final HGHandle handle);
    
	/**
	 * <p>Freezing an atom in the cache would prevent it from ever being removed.</p>
	 * 
	 * @param handle The <code>HGLiveHandle</code> of the atom to be frozen. The atom
	 * must already be in the cache.
	 */
	void freeze(HGLiveHandle handle);

	/**
	 * <p>Unfreezing a previously frozen atom makes it available for eviction. It is ok
	 * to unfreeze an atom that has never been frozen.  
	 * </p>
	 * 
	 * @param handle The <code>HGLiveHandle</code> of the atom to be unfrozen. The atom
	 * must be in the cache.
	 */
	void unfreeze(HGLiveHandle handle);

	/**
	 * <p>Find out whether a given atom is frozen in the cache.</p>
	 * 
	 * @param handle The live handle of the atom.
	 * @return <code>true</code> if the atom is frozen (i.e. would never be evicted from the cache)
	 * and <code>false</code> otherwise.
	 */
	boolean isFrozen(HGLiveHandle handle);
	
	/**
	 * <p>Close the cache. This will clear the cache, completely and perform any
	 * cleaning operations such as unloading of atoms and the like.</p>
	 * 
	 * <p>
	 * Once the cache is closed, it cannot be used again.
	 * </p>
	 * 
	 * <p>
	 * This method is normally invoked by HyperGraph only. The method is not guaranteed
	 * to be thread-safe. It can rely on the fact that no other threads are accessing 
	 * the cache while it is executing.
	 * </p>
	 */
	void close();
	
	/**
	 * <p>Return the incidence set cache. The incidence set cache is maintained separately
	 * from the main atom cache. Incidence sets don't hold actual atoms, but only their
	 * handles. Also, they are ordered and can be queried for membership efficiently.
	 */
	HGCache<HGPersistentHandle, IncidenceSet> getIncidenceCache();
	
	/**
	 * <p>
	 * Set the implementation of the incidence sets cache to use.
	 * </p>
	 * @param cache
	 */
	void setIncidenceCache(HGCache<HGPersistentHandle, IncidenceSet> cache);
}
