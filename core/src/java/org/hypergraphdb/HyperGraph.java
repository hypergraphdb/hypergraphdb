/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.hypergraphdb.cache.WeakRefAtomCache;
import org.hypergraphdb.handle.DefaultLiveHandle;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.handle.HGManagedLiveHandle;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.type.*;
import org.hypergraphdb.atom.HGStats;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.event.*;
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.util.HGLogger;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * This is the main class representing a HyperGraph database. A HyperGraph database resides
 * in a dedicated folder and can be accessed by more than one process. Also, a single process
 * may open several databases at once.    
 * </p>
 * 
 * <p>
 * Because HyperGraphDB is primarely an embedded database, it does not rely on a client-server
 * model and therefore there is no notion of a connection. The database is accessed by
 * instantiating an instance of this class and then using its methods. An instance of this
 * class is essentially a connection to the database and it must be always properly closed
 * before application shutdown. To close the database, call the <code>close</code> method. 
 * When the application exits abruptly, without having a chance to close, it may fail to
 * open subsequently without running a recovery process. A light recovery process is performed
 * every time you open a database, but sometimes a full recovery may be needed in which case
 * a separate command line utility must be run as documented in the reference guide.
 * </p>  
 *   
 * <p>
 * This class encapsulates and offers access to all important HyperGraphDB top-level objects such as
 * its associated <code>HGTransactionManager</code>, <code>HGTypeSystem</code>, 
 * <code>HGEventManager</code> and the low-level <code>HGStore</code>.
 * </p>
 * 
 * <p>
 * Each datum in a HyperGraph database is called an <code>atom</code>. Atoms are either
 * arbitrary plain objects or instances of <code>HGLink</code>. Using this class, you may:
 * 
 * <ul>
 * <li>Add new atoms with the <code>add</code> family of methods.</li>
 * <li>Remove existing atoms with the <code>remove</code> method.</li>
 * <li>Change the value of an atom while preserving its HyperGraph handle (i.e. its
 * <em>database id</em>, if you will) with the <code>replace</code> family of methods.</li>
 * <li>Add new atoms with existing handles with the <code>define</code> family of methods. 
 * This is useful, for example, when moving atoms from one HyperGraphDB to another.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * An important aspect of HyperGraph atoms are their associated system flags. More information
 * about what those are and how to use them can be found in the {@link HGSystemFlags} class.
 * </p>
 * 
 * <p>
 * For aggregate structures such as Java beans, it is often useful to create indices
 * to speed up searching by certain properties. You can manipulate indices with the
 * {@link HGIndexerManager} associated with a HyperGraph instance. Call <code>getIndexManager</code>
 * to get the index manager.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public /*final*/ class HyperGraph
{
    public static final HGHandle [] EMTPY_HANDLE_SET = new HGHandle[0];
    public static final LazyRef<HGHandle[]> EMTPY_HANDLE_SET_REF = new ReadyRef<HGHandle[]>(new HGHandle[0]);
    public static final HGPersistentHandle [] EMPTY_PERSISTENT_HANDLE_SET = new HGPersistentHandle[0];
    
    
    private static final String TYPES_CONFIG_FILE = "/org/hypergraphdb/types";
    private static final String TYPES_INDEX_NAME = "HGATOMTYPE";
    private static final String VALUES_INDEX_NAME = "HGATOMVALUE";
    private static final String SA_DB_NAME = "HGSYSATTRIBS";
    
    /**
     * The location (full directory path on disk) for this HyperGraph database. 
     */
    private String location = null;
    
    /**
     * Is a database currently open?
     */
    private boolean is_open = false;
    
    /**
     * The hypergraph store. Manages low-level persistence operations and
     * indexing.
     */
    private HGStore store = null;
    
    /**
     * An index manager for user created indices. 
     */
    private HGIndexManager idx_manager = null;
    
    /**
     * The hypergraph typing manager. Integrates tightly with a HyperGraph
     * instance.
     */
    private HGTypeSystem typeSystem = null;
   
    /**
     * The HyperGraph atom cache. 
     */
    HGAtomCache cache = null;

    /**
     * The event manager handles event listener registration and even dispatching.
     */
    HGEventManager eventManager = null;

    
    /**
     * A logger for HyperGraph.
     */
    final HGLogger logger = new HGLogger();
    
    //-------------------------------------------------------------------------
    // DEFAULT INDICES
    //-------------------------------------------------------------------------
    /**
     * Each atom is indexed by its type handle.
     */
    HGIndex<HGPersistentHandle, HGPersistentHandle> indexByType = null;
    
    /**
     * Each atom is indexed by its value handle as well.
     */
    HGIndex<HGPersistentHandle, HGPersistentHandle> indexByValue = null;

    /**
     * An index holding system attributes for atoms with different than default 
     * system flags.
     */
    HGIndex<HGPersistentHandle, AtomAttrib> systemAttributesDB = null;
    
    HGHandle statsHandle = null;
    HGStats stats = new HGStats();
    
    public HyperGraph()
    {    	
    }
    
    /**
     * <p>Construct a run-time instance of a hypergraph database and open the database
     * at the specified location.</p>
     * 
     * @param location The full path of the directory where the database resides. 
     */
    public HyperGraph(String location)
    {
    	open(location);
    }

    /**
     * <p>Open the database at the specified location.</p>
     * 
     * @param location The full path of the directory where the database resides.
     */
    public synchronized void open(String location)
    {
    	if (this.location != null && this.location.length() > 0)
    		HGEnvironment.remove(this.location);
    	this.location = location;
    	open();
    	HGEnvironment.set(this.location, this);
    }
    
    /** 
     * <p>Return the physical location of this HyperGraph database. The location is
     * set either at construction or by a call to the <code>open(String location)</code>
     * method.
     * </p>
     */
    public String getLocation()
    {
    	return location;
    }
     
    /**
     * <p>Open the database if it's not already open.</p>
     */
    private synchronized void open()
    {
    	if (is_open)
    		close();
    	is_open = true;
    	try
    	{
    		eventManager = new HGEventManager();
	        store = new HGStore(location);
	        cache = new WeakRefAtomCache();
	        cache.setHyperGraph(this);
	        typeSystem = new HGTypeSystem(this);    
	        
	        //
	        // Make sure system indices are created.
	        //
	        indexByType = store.getIndex(TYPES_INDEX_NAME, BAtoHandle.getInstance(), BAtoHandle.getInstance(), null);
	        						     
	        if (indexByType == null)
	            indexByType = store.createIndex(TYPES_INDEX_NAME, BAtoHandle.getInstance(), BAtoHandle.getInstance(), null);
	        indexByValue = store.getIndex(VALUES_INDEX_NAME, BAtoHandle.getInstance(), BAtoHandle.getInstance(), null);
	        if (indexByValue == null)
	        	indexByValue = store.createIndex(VALUES_INDEX_NAME, BAtoHandle.getInstance(), BAtoHandle.getInstance(), null);
	        
	        systemAttributesDB = store.getIndex(SA_DB_NAME, BAtoHandle.getInstance(), AtomAttrib.baConverter, null);
	        if (systemAttributesDB == null)
	        {
	        	
	        	systemAttributesDB = store.createIndex(SA_DB_NAME, BAtoHandle.getInstance(), AtomAttrib.baConverter, null);
	        }     
	                    
	        idx_manager = new HGIndexManager(this);
	        
	        //
	        // Now, bootstrap the type system.
	        //
	        typeSystem.bootstrap(TYPES_CONFIG_FILE);                 
            
    		idx_manager.loadIndexers();
    		
	        // Initialize atom access statistics, purging and the like. 
	        initAtomManagement();    
            
	        // Load all listeners stored in this HyperGraph as HGListenerAtoms
	        loadListeners();

	        eventManager.dispatch(this, new HGOpenedEvent());
	    	this.location = location;
    	}
    	catch (Throwable t)
    	{
    		if (store != null) try { store.close(); } catch (Throwable t1) { }
    		try { cache.close(); } catch (Throwable t1) { }
    		is_open = false;
    		throw new HGException(t);
    	}    	
    }
    
    /**
     * <p>Gracefully close all resources associated with the run-time instance
     * of <code>HyperGraph</code>. </p>
     */
    public void close()
    {
        if (!is_open)
            return;
        ArrayList<Throwable> problems = new ArrayList<Throwable>();
        try { eventManager.dispatch(this, new HGClosingEvent()); } catch (Throwable t) { problems.add(t); }
    	try { replace(statsHandle, stats);  					 } catch (Throwable t) { problems.add(t); }        
        try { cache.close(); 									 } catch (Throwable t) { problems.add(t); }        
    	try { idx_manager.close();								 } catch (Throwable t) { problems.add(t); }
    	try { eventManager.clear();								 } catch (Throwable t) { problems.add(t); }
        try { store.close();								 	 } catch (Throwable t) { problems.add(t); }
        is_open = false;
        for (Throwable t : problems)
        {
        	System.err.println("Problem during HyperGraph close, stack trace of exception follows:");
        	t.printStackTrace(System.err);
        }
    }
    
    /** 
     * <p>Return <code>true</code> is the database is currently open and <code>false</code>
     * otherwise.</p>
     */
    public boolean isOpen()
    {
    	return is_open;
    }
    
    /**
     * <p>Return the <code>HGStore</code> used by this hypergraph.</p>
     */
    public HGStore getStore()
    {
        return store;
    }
    
    /**
     * <p>Return the <code>HGTransactionManager</code> associated with this
     * HyperGraph.
     * </p>
     * <p>
     * The transaction manager allows you to encapsulate several operations
     * as a single, atomic transaction. 
     * </p>
     */
    public HGTransactionManager getTransactionManager()
    {
    	return store.getTransactionManager();
    }
    
    /**
     * <p>Return the <code>HGTypeSystem</code> instance managing run-time types
     * for this hypergraph.</p>
     */
    public HGTypeSystem getTypeSystem()
    {
        return typeSystem;
    }
    
    /**
     * <p>Return this <code>HyperGraph</code>'s event manager instance.</p>
     */
    public HGEventManager getEventManager()
    {
    	return eventManager;
    }
    
    /**
     * <p>Return the persistent handle of a given atom. Generally, when working
     * with atom handles, one needn't worry wether they are in-memory or not. However,
     * some low-level APIs dealing with permanent storage explicitely require 
     * a <code>HGPersistentHandle</code> and, in addition, applications may not to record
     * the permanent handle of an atom somewhere else. 
     * </p>
     *
     * @param handle The <code>HGHandle</code> of the atom. 
     * @return The <code>HGPersistentHandle</code> corresponding to the passed in
     * <code>HGHandle</code>.
     */
    public HGPersistentHandle getPersistentHandle(HGHandle handle)
    {
        if (handle instanceof HGPersistentHandle)
            return (HGPersistentHandle)handle;
        else
            return ((HGLiveHandle)handle).getPersistentHandle();
    }
    
    /**
     * <p>Return <code>true</code> if a given is currently loaded in main memory
     * and <code>false</code> otherwise.</p>
     * 
     * @param handle The handle of the atom.
     */
    public boolean isLoaded(HGHandle handle)
    {
    	if (handle instanceof HGPersistentHandle)
    		return cache.get(handle) != null;
    	else
    		return true;
    }
    
    /**
     * <p>Return <code>true</code> if a given is currently frozen in the cache
     * and <code>false</code> otherwise. Frozen atoms are guarantueed to NOT be evicted
     * from the cache.</p>
     * 
     * @param handle The handle of the atom.
     */
    public boolean isFrozen(HGHandle handle)
    {
        HGLiveHandle lHandle =  (handle instanceof HGPersistentHandle) ? cache.get((HGPersistentHandle)handle) : (HGLiveHandle)handle;
        return (lHandle == null) ? false : cache.isFrozen(lHandle);
    }

    /**
     * <p>
     * Freeze an atom into the HyperGraph cache.Frozen atoms are guarantueed to NOT be evicted
     * from the cache. If the atom is not already currently loaded, it will be loaded. If it is
     * already frozen, nothing will be done - so, it's safe to call this method multiple times.
     * Because the atom instance is returned, this method may be called instead of <code>get</code>
     * to retrieve an atom instance based on its handle while making sure that it's never removed
     * from the cache. 
     * </p>
     * 
     * <p>
     * This method should be called with care since it is a possible source of memory leaks. The
     * <code>unfreeze</code> method should be called at appropriate times when you no longer need
     * an object to absolutely remain in main memory. Typically, freezing an atom is desirable in
     * the following situations:
     * 
     * <ul>
     * <li>You need to retrieve a <code>HGHandle</code> from a Java instance reference by a call
     * to the <code>getHandle</code>. This is only guarantueed to work when the atom is in the cache.</li>
     * <li>The atom is a very large object, expansive to re-construct from permanent storage and even
     * though used relatively rarely, it's better is it remain in memory. The cache will normally
     * evict atoms based on used, not on they memory footprint which would be too much overhead
     * to estimate in Java.</li>
     * </ul>
     * </p>
     * 
     * @param handle The handle of the atom.
     * @return The instance of the atom that was frozen.
     */
    public Object freeze(HGHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("Trying to freeze null atom handle.");
        get(handle);
        HGLiveHandle lHandle =  (handle instanceof HGPersistentHandle) ? cache.get((HGPersistentHandle)handle) : (HGLiveHandle)handle;
        while (lHandle == null)
        {
            get(handle);
            lHandle = cache.get((HGPersistentHandle)handle);
        }
        if (!cache.isFrozen(lHandle))
            cache.freeze(lHandle);
        return lHandle.getRef();
    }
    
    /**
     * <p>
     * Unfreeze a previously frozen atom so that it becomes subject of eviction from the cache.
     * If the atom is not loaded or not currently frozen in the cache, nothing is done - so, it's
     * safe to call this method multiple times. 
     * </p>
     * 
     * @param handle
     */
    public void unfreeze(HGHandle handle)
    {
        HGLiveHandle lHandle =  (handle instanceof HGPersistentHandle) ? cache.get((HGPersistentHandle)handle) : (HGLiveHandle)handle;
        if (lHandle != null && cache.isFrozen(lHandle))
            cache.unfreeze(lHandle);
    }
    
    /**
     * <p>Return <code>true</code> if the incidence set of a given atom is currently
     * loaded in main memory and <code>false</code> otherwise.</p>
     * 
     * @param h The handle of the atom in whose incidence we are interested.
     */
    public boolean isIncidenceSetLoaded(HGHandle h)
    {
    	return cache.getIncidenceSet(getPersistentHandle(h)) != null;
    }
    
    /**
     * <p>Add a new atom to the database using the default, Java Beans based typing
     * mechanism and default system flags.
     * </p>
     * 
     * @param atom The <code>Object</code> instance to be stored as a hypergraph atom.
     * @return A <code>HGHandle</code> to the newly created atom. The handle may be used
     * as a reference to the atom within hypergraph and to construct link to that atom.
     */
    public HGHandle add(Object atom)
    {
    	return add(atom, 0);
    }
    
    /**
     * <p>Add a new atom to the database using the default, Java Beans based typing
     * mechanism and the set of specified system flags. The atom to be added will be 
     * treated as a Java bean with appropriate getter and setter methods used as 
     * property accessors. The type of the atom will be inferred using the Java 
     * instrospection mechanism.
     * </p>
     * 
     * @param atom The <code>Object</code> instance to be stored as a hypergraph atom.
     * @param flags A combination of system-level bit flags. Available flags that can 
     * be <em>or-ed</em> together are listed in the <code>HGSystemFlags</code> interface.   
     * @return A <code>HGHandle</code> to the newly created atom. The handle may be used
     * as a reference to the atom within hypergraph and to construct link to that atom.
     */
    public HGHandle add(Object atom, int flags)
    {
    	HGHandle result;
    	
        if (atom instanceof HGLink)
        {
            HGLink link = (HGLink)atom;
            Object value = link;
            if (link instanceof HGValueLink)
                value = ((HGValueLink)link).getValue();            
            HGHandle type = typeSystem.getTypeHandle(value.getClass());
            result = addLink(value, type, link, (byte)flags);
        }
        else
            result = addNode(atom, typeSystem.getTypeHandle(atom.getClass()), (byte)flags);
        eventManager.dispatch(this, new HGAtomAddedEvent(result));
        return result;
    }
    
    /**
     * <p>Add a new atom with a specified type and default system flags to the database.</p>
     * 
     * @param atom The new atom to add.
     * @param type The handle of the <em>HyperGraphDB</em> type of the atom. This type must have
     * been previously registered with the type system. 
     */
    public HGHandle add(Object atom, HGHandle type)
    {
    	return add(atom, type, 0);
    }
    /**
     * <p>Add a new atom with a specified type and system flags to the database.</p>
     * 
     * @param atom The new atom to add.
     * @param type The handle of the <em>HyperGraphDB</em> type of the atom. This type must have
     * been previously registered with the type system. 
     * @param flags A combination of system-level bit flags. Available flags that can 
     * be <em>or-ed</em> together are listed in the <code>HGSystemFlags</code> interface.   
     * @return The HyperGraph handle of the newly added atom.
     */
    public HGHandle add(Object atom, HGHandle type, int flags)
    {
    	HGHandle result;
        if (atom instanceof HGLink)
        {
            HGLink link = (HGLink)atom;
            Object value = link;
            if (link instanceof HGValueLink)
                value = ((HGValueLink)link).getValue();            
            result = addLink(value, type, link, (byte)flags);
        }
        else
            result = addNode(atom, type, (byte)flags);
        eventManager.dispatch(this, new HGAtomAddedEvent(result));
        return result;
    }
    
    /**
     * <p>
     * Refresh an atom handle with a currently valid and efficient run-time value. HyperGraph
     * manages essentially two types of handles: run-time handles that are reminiscent to memory
     * pointers and provide very fast access to loaded atoms and persistent handles that refer
     * to long term storage. An atom can be accessed with both types of handles at all times
     * regardless of whether it is currently in memory or "passified" into permanent storage. 
     * During long operations on a large graph, it is likely that atoms get moved in and out
     * of main memory and their <em>live</em> status constantly fluctuates. This method allows
     * you to bring a <code>HGHandle</code> in line with the current <em>live</em> status of 
     * an atom. It is desirable to bring a <code>HGHandle</code> when the atom will be accessed
     * frequently. The following scenarios describe situation where it may be worth refreshing
     * a handle: 
     * </p>
     * 
     * <ul>
     * <li>A link has been retrieved and the atoms in its target set will be frequently
     * accessed. Generally, the atom handles comprising the target set will be persistent handles,
     * unless those atoms are loaded at the time the link is retrieved. Therefore, one may
     * want to refresh that target handles after their corresponding atoms have been loaded 
     * in memory.</li>
     * <li>An atom was loaded, but it wasn't used for a long time and was therefore removed 
     * from the cache. In this case, its live handle would loose its "liveliness" and it would
     * be a good idea to refresh it if the atom starts being used again.
     * </ul>
     * 
     * <p>
     * Given a persistent handle as an argument, this method will return the corresponding live handle 
     * if and only if the atom currently loaded. Given an invalid live handle, the method will
     * either return a new, valid one if the atom was re-loaded, or it will simply return 
     * its argument if the atom is currently in the cache.  
     * </p>
     * 
     * @param handle The handle of the atom.
     * @return An updated handle according to the behavior described above.
     */
    public HGHandle refreshHandle(HGHandle handle)
    {
    	if (handle instanceof HGPersistentHandle)
    	{
	    	HGHandle result = cache.get((HGPersistentHandle)handle);
	    	return result != null ? result : handle;
    	}
    	else
    	{
    		HGLiveHandle live = (HGLiveHandle)handle;
    		if (live.getRef() == null)
    		{
    			HGLiveHandle updated = cache.get(live.getPersistentHandle());
    			if (updated != null)
    				return updated;
    			else
    				return live.getPersistentHandle();
    		}
    		else
    			return handle;
    	}
    }
    
    /**
     * <p>Retrieve a hypergraph atom by its handle.</p>
     * <p>HyperGraph will immediately return
     * an atom available in memory through a live handle, and it will fetch
     * the atom from the store for a persistent handle.</p>
     *  
     * @param handle The handle of the atom.
     * @return The hypergraph atom. HyperGraph will construct a run-time instance 
     * of the appropriate type, but it is up to the application logic to perform
     * the right cast. The actual type of the atom may be obtained via a call
     * to <code>HyperGraph.getTypeSystem().getAtomType(Object)</code>.
     */
    public <T> T get(HGHandle handle)
    {
    	stats.atomAccessed();
    	
    	HGLiveHandle liveHandle = null;
        HGPersistentHandle persistentHandle = null;
        if (handle instanceof HGLiveHandle)
        	liveHandle = (HGLiveHandle)handle;
        else
        	liveHandle = cache.get((HGPersistentHandle)handle);
        
        if (liveHandle != null)
        {
        	T theAtom = (T)liveHandle.getRef();
        	if (theAtom == null)
        	{
        		//
        		// The atom has been evicted from the cache, so the live reference is no
        		// longer valid. We have to rely on the persistent handle reference.
        		HGLiveHandle existing = cache.get(liveHandle.getPersistentHandle());
        		if (existing != null)
        		{
        			theAtom = (T)existing.getRef();
        			if (theAtom != null)
        			{
        				eventManager.dispatch(this, new HGAtomAccessedEvent(existing, theAtom));
        				return (T)theAtom;
        			}
        		}
       			persistentHandle = liveHandle.getPersistentHandle();
        	}
        	else
        	{
        		eventManager.dispatch(this, new HGAtomAccessedEvent(liveHandle, theAtom));
        		return (T)theAtom;
        	}
        }
        else
            persistentHandle = (HGPersistentHandle)handle;
        
        Pair<HGLiveHandle, Object> loaded = loadAtom(persistentHandle, liveHandle);                
        
        if (loaded == null)
        	return null; // TODO: perhaps we should throw an exception here, but a new type, e.g. HGInvalidHandleException?
        
        liveHandle = loaded.getFirst();
        
        //
        // If the incidence set of the newly fetched atom is already loaded,
        // traverse it to update the target handles of all links pointing to it.
        //
        HGHandle [] incidenceSet = cache.getIncidenceSet(persistentHandle);
        if (incidenceSet != null)
            for (int i = 0; i < incidenceSet.length; i++)
                if (incidenceSet[i] instanceof HGLiveHandle)
                {
                    HGLink incidenceLink = (HGLink)((HGLiveHandle)incidenceSet[i]).getRef();
                    if (incidenceLink != null) // ref may be null because of cache eviction
                    	updateLinkLiveHandle(incidenceLink, liveHandle);
                }
        //
        // If the newly fetched atom is a link, update all loaded incidence
        // sets, of which it is part, with its live handle. 
        //
        if (liveHandle.getRef() instanceof HGLink)
        {
        	HGLink link = (HGLink)liveHandle.getRef();
            for (int i = 0; i < link.getArity(); i++)
            {
                HGHandle [] targetIncidenceSet = cache.getIncidenceSet(getPersistentHandle(link.getTargetAt(i)));
                if (targetIncidenceSet != null)
                    for (int j = 0; j < targetIncidenceSet.length; j++)
                    {
                        if (targetIncidenceSet[j].equals(persistentHandle))
                        	targetIncidenceSet[j] = liveHandle;
                    }
            }
        }
        
        eventManager.dispatch(this, new HGAtomAccessedEvent(liveHandle, loaded.getSecond()));
        
        return (T)loaded.getSecond();
    }

    /**
     * <p>Return the handle of the specified atom.</p>
     * 
     * @param atom The atom whose handle is desired.
     * @return The <code>HGHandle</code> of the passed in atom, or <code>null</code>
     * if the atom is not in HyperGraph.
     */
    public HGHandle getHandle(Object atom)
    {
        return cache.get(atom);
    }

    /**
     * <p>Retrieve the handle of the type of the atom refered to by <code>handle</code>.</p>
     * 
     * <p><strong>FIXME:</strong> Instances of the same run-time Java type are not guarantueed
     * to have the same HyperGraph type. For instance, a Java <code>String</code> may be mapped
     * either to a HyperGraph indexed and reference counted strings, or to long text blobs. Therefore,
     * the correct way of getting the actual HG type of an atom is by reading of off storage. We 
     * don't cache type handles of atoms as of now and this might turn out to be a performance issue.
     * </p.
     *  
     * @param handle The <code>HGHandle</code> of the atom whose type is desired.
     * @return The <code>HGHandle</code> of the atom type.
     * @throws HGException if the passed in handle is invalid or unknown to HyperGraph.
     */
    public HGHandle getType(HGHandle handle)
    {
    	HGPersistentHandle pHandle;
    	if (handle instanceof HGLiveHandle)
    	{
    		pHandle = ((HGLiveHandle)handle).getPersistentHandle();
    	}
    	else
    	{
    		pHandle = (HGPersistentHandle)handle;
    	}
    	
    	HGPersistentHandle [] link = store.getLink(pHandle);
    	if (link == null || link.length < 2)
    		return null;
    	else
    		return refreshHandle(link[0]);
    }

    /**
     * <p>Remove an atom from the HyperGraph database. This is equivalent to calling
     * <code>remove(handle, true)</code> - see that version of <code>remove</code> for
     * detailed explanation.</p>
     * 
     * @param handle
     * @return <code>true</code> if the atom was successfully removed and <code>false</code>
     * otherwise.
     */
    public boolean remove(final HGHandle handle)
    {
    	return remove(handle, true);
    }
    
    /**
     * <p>Remove the atom referred to by <code>handle</code> from the hypergraph store.</p>
     * 
     * <p>
     * <strong>Note:</strong> This operation will delete the atom and potentially
     * recursively all links pointing to it together with links pointing to them etc., if
     * the <code>keepIncidentLinks</code> parameter is <code>false</code>. 
     * Also, if the atom
     * is a type, all its instances are removed recursively (however, it is not possible to 
     * remove a predefined type). Thus, theoretically the removal of an atom could empty 
     * the whole hypergraph database. 
     * </p>
     * 
     * <p>
     * When dealing with a complex linked structure, it is never obvious when incident links
     * should be removed along with a given atom. As a rule of thumb, if the links are
     * interpreted as ordered, in general they should be removed. Otherwise, if they are
     * unordered, the atom should simply be removed from their target set. Note, however,
     * that this very much depends on the application semantics. In mixed cases, when
     * some links pointing to the atom must be removed, but others must be preserved, one
     * should delete the former "manually" and set the <code>keepIncidentLinks</code> to 
     * <code>true</code>.  
     * </p>
     * 
     * <p>
     * <strong>Note:</strong> also that the <code>keepIncidentLinks</code> applies recursively. 
     * Thus, if you set it to <code>false</code> links pointing to the links pointing to...etc.
     * the given atom will all be deleted.
     * </p>
     * 
     * @param handle The handle of the atom to be removed. <strong>NOTE:</strong> if no atom
     * exists with this handle (e.g. the atom was already removed), the method does nothing and
     * throws no exception. If an attempt is made to remove an atom with a <code>null</code>
     * handle, then a regular <code>NullPointerException</code> is thrown.
     * @param keepIncidentLinks A flag indicating whether to remove the atom from the links
     * pointing to it (if <code>true</code>) or whether to remove the links altogether (if
     * <code>false</code>). The flag applies recursively to all removals triggered from
     * this call.
     * @return <code>true</code> if the atom was successfully removed and <code>false</code>
     * otherwise.
     */
    public boolean remove(final HGHandle handle, final boolean keepIncidentLinks)
    {
    	if (eventManager.dispatch(this, new HGAtomRemoveRequestEvent(handle)) == HGListener.Result.cancel)
    		return false;
    	
    	getTransactionManager().transact(new Callable<Object>() 
    	{ public Object call() { removeTransaction(handle, keepIncidentLinks); return null; }});
    	return true;
    }
    
    private void removeTransaction(final HGHandle handle, final boolean keepIncidentLinks)
    {
        HGPersistentHandle pHandle = null;
        HGLiveHandle lHandle = null;
        if (handle instanceof HGLiveHandle)
        {
            lHandle = (HGLiveHandle)handle;            
            pHandle = lHandle.getPersistentHandle(); 
        }
        else
        {
            pHandle = (HGPersistentHandle)handle;
            lHandle = cache.get(pHandle);
        }

        HGPersistentHandle [] layout = store.getLink(pHandle);        
        
        if (layout == null)
            return;
        else if (layout[0].equals(HGTypeSystem.TOP_PERSISTENT_HANDLE))
        	throw new HGException("Cannot remove the HyperGraph primitive type: " + pHandle);
        
        Object atom = get(handle); // need the atom in order to clear all indexes...
        
        //
        // If the atom is a type, remove it from the type system 
        // (which also removes all its instances).
        //
        if (atom instanceof HGAtomType)
        {	        		
        	HGSearchResult instances = null;
        	try
        	{
        		instances = indexByType.find(pHandle);
	        	while (instances.hasNext())
	        		removeTransaction((HGPersistentHandle)instances.next(), keepIncidentLinks);
        	}
        	finally
        	{
        		if (instances != null) instances.close();
        	}
            idx_manager.unregisterAll(pHandle);
        	typeSystem.remove(pHandle, (HGAtomType)atom);	        	
        }
        
        HGPersistentHandle typeHandle = layout[0];
        HGPersistentHandle valueHandle = layout[1];
        HGAtomType type = typeSystem.getType(typeHandle);	        
        HGHandle [] incidenceSet = cache.getIncidenceSet(pHandle);
        
        //
        // Clean all indexing entries related to this atom.
        //
        idx_manager.maybeUnindex(typeHandle, type, atom, pHandle);        
        indexByType.removeEntry(typeHandle, pHandle);
        indexByValue.removeEntry(valueHandle, pHandle);

        //
        // Remove the atom record from the store and cache.
        //
        TypeUtils.releaseValue(HyperGraph.this, valueHandle);
        type.release(valueHandle);         
        store.removeLink(pHandle);
        if (lHandle != null)
            cache.remove(lHandle);
        
        //
        // If it's a link, remove it from the incidence sets of all its 
        // targets.
        //
        if (layout.length > 2)
            for (int i = 2; i < layout.length; i++)
            	removeFromIncidenceSet(layout[i], lHandle, pHandle);

        //
        // Handle links pointing to this atom:
        //
        if (incidenceSet != null)
        {
	        for (int i = 0; i < incidenceSet.length; i++)
		        if (!keepIncidentLinks)
		        	removeTransaction(incidenceSet[i], keepIncidentLinks);
		        else
		        	targetRemoved(incidenceSet[i], pHandle);
        }
        else
        {
        	HGSearchResult<HGHandle> irs = null;
        	try
        	{
        		irs = store.getIncidenceResultSet(pHandle);
        		while (irs.hasNext())
        		{
        			HGHandle link = irs.next();
			        if (!keepIncidentLinks)
			        	removeTransaction(link, keepIncidentLinks);
			        else
			        	targetRemoved(link, pHandle);	        			
        		}
        	}
        	finally
        	{
        		HGUtils.closeNoException(irs);
        	}
        }
        cache.removeIncidenceSet(pHandle);        
        store.removeIncidenceSet(pHandle);
        eventManager.dispatch(HyperGraph.this, new HGAtomRemovedEvent(pHandle));    	
    }
    
    /**
     * <p>
     * Update the value of an atom in HyperGraph. This is equivalent to
     * a call to <code>replace(getHandle(atom), atom)</code>. An exception is
     * thrown if the handle of the passed in atom could not be found. 
     * </p>
     * 
     * @param atom
     */
    public void update(Object atom)
    {
    	HGHandle h = getHandle(atom);
    	if (h == null)
    		throw new HGException("Could not find HyperGraph handle for atom " + atom);
    	else
    		replace(h, atom);
    }
    
    /**
     * <p>
     * Replace the value of an atom with a new value. The atom will preserve
     * its handle and all links pointing to it will remain valid. This method allows
     * you to effectively change the type of an atom while preserving the hypergraph
     * structure intact. The new value does not have to be of a different than
     * the old value type, of course.
     * </p>
     * 
     * <p>
     * As when adding new atom of arbitrary Java types, the concrete type of the
     * <code>atom</code> parameter will be inferred if necessary.
     * </p>
     *
     * <p>
     * The structure of the graph will be modified only if the atom is 
     * replaced with a new value that has a different linking import 
     * (e.g. a node is replaced by a link or vice-versa, or a link with
     * a different target set is being replaced).
     * </p>
     *  
     *  <p>
     *  <strong>NOTE:</strong> If a <code>HGAtomType</code> atom with a non-empty 
     *  instance set is being replaced, the new value must also be an <code>HGAtomType</code>
     *  that is <em>compatible</em> with the old <code>HGAtomType</code>. Compatibility
     *  here means that the new type must be able to store all values of the old type.
     *  The <code>replace</code> method will effectively attempt to recursively morph
     *  all those values based on the the new <code>HGAtomType</code>.  
     *  </p>
     *  
     * @param handle The handle of the atom to be replaced.
     * @param atom An arbitrary Java <code>Object</code> representing the new atom. The
     * type of the atom will be inferred using the Java instrospection mechanism.
     */
    public void replace(HGHandle handle, Object atom)
    {
    	HGHandle atomType;
    	if (atom instanceof HGValueLink)
    		atomType = typeSystem.getTypeHandle(((HGValueLink)atom).getValue().getClass());
    	else
    		atomType = typeSystem.getTypeHandle(atom.getClass());
        replace(handle, atom, atomType);
        eventManager.dispatch(this, new HGAtomReplacedEvent(handle));        
    }
    
    /**
     * <p>
     * Replace the value of an atom with a new value. The atom will preserve
     * its handle and all links pointing to it will remain valid. This method allows
     * you to effectively change the type of an atom while preserving the hypergraph
     * structure intact. The new value does not have to be of a different than
     * the old value type, of course.
     * </p>
     * 
     * <p>
     * The structure of the graph will be modified only if the atom is 
     * replaced with a new value that has a different linking import 
     * (e.g. a node is replaced by a link or vice-versa, or a link with
     * a different target set is being replaced).
     * </p>
     *  
     *  <p>
     *  <strong>NOTE:</strong> If a <code>HGAtomType</code> atom with a non-empty 
     *  instance set is being replaced, the new value must also be an <code>HGAtomType</code>
     *  that is <em>compatible</em> with the old <code>HGAtomType</code>. Compatibility
     *  here means that the new type must be able to store all values of the old type.
     *  The <code>replace</code> method will effectively attempt to recursively morph
     *  all those values based on the the new <code>HGAtomType</code>.  
     *  </p>
     *
     *  <p>
     *  If an attempt is made to replace one of the predefined type atoms, the behavior
     *  is currently undefined.
     *  </p>
     *  
     * @param handle The handle of the atom to be replaced.
     * @param atom An arbitrary Java <code>Object</code> representing the new atom.
     * @param type The type of the new atom value. 
     */
    public void replace(HGHandle handle, Object atom, HGHandle type)
    {
        HGPersistentHandle pHandle = null;
        HGLiveHandle lHandle = null;
        if (handle instanceof HGPersistentHandle)
        {
            pHandle = (HGPersistentHandle)handle;
        	lHandle = cache.get(pHandle);            
        }
        else
        {
        	lHandle = (HGLiveHandle)handle;
            pHandle = lHandle.getPersistentHandle();
        }
        
        replaceInternal(lHandle, pHandle, atom, type);
        eventManager.dispatch(this, new HGAtomReplacedEvent(lHandle));        
    }
    
    /**
     * <p>
     * Put an existing atom into this HyperGraph instance. This is a rather low-level method
     * that requires you to explicitely find the type and value handles for the atom and use
     * an already existing, yet unknown to this HyperGraph instance, persistent handle. 
     * </p>
     * 
     * <p>
     * One possible use of this is when an application relies on a HyperGraph for storage and it needs
     * to populate it with some predefined set of atoms with a set of existing, prefabricated handles.
     * Using handles in an application instead of some naming scheme and the corresponding <em>name</em>
     * properties is the preferred way of working with HyperGraph.
     * </p>
     * 
     * @param atomHandle A valid <code>HGPersistentHandle</code> of the atom being defined. If an atom
     * already exists with this handle, it will be replaced. This parameter cannot be <code>null</code>.
     * @param typeHandle The handle of this atom's type. The corresponding <code>HGAtomType</code> should
     * be capable of retrieving the actual atom instance based on the passed in <code>valueHandle</code>.
     * This parameter cannot be <code>null</code>.
     * @param valueHandle The handle of the atom's value. This parameter cannot be <code>null</code>.
     * @param outgoingSet If the atom is a link, this parameter specifies the set of atoms pointed to by the link.
     * If this parameter is <code>null</code> or of size 0, then the atom is not a link.
     */
    public void define(final HGPersistentHandle atomHandle, 
    				   final HGHandle typeHandle, 
    				   final HGHandle valueHandle, 
    				   final HGHandle [] outgoingSet)
    {
    	getTransactionManager().transact(new Callable<Object>() 
    	{ public Object call() {
	    	HGPersistentHandle [] layout = new HGPersistentHandle[outgoingSet == null ? 2 : 2 + outgoingSet.length];
	    	layout[0] = getPersistentHandle(typeHandle);
	    	layout[1] = getPersistentHandle(valueHandle);
	    	if (outgoingSet != null)
	    		for (int i = 0; i < outgoingSet.length; i++)
	    			layout[i + 2] = getPersistentHandle(outgoingSet[i]);
	    	store.store(atomHandle, layout);
	    	indexByType.addEntry(layout[0], atomHandle);
	    	indexByValue.addEntry(layout[1], atomHandle);	    	
	    	return null;
    	}});
    }
    
    /**
     * <p>
     * Put an atom with a specific <code>HGPersistentHandle</code> into this HyperGraph instance.
     * </p>
     * 
     * <p>
     * One possible of this is when an application relies on a HyperGraph for storage and it needs
     * to populate it with some predefined set of atoms with a set of existing, prefabricated handles.
     * Using handles in an application instead of some naming scheme and the corresponding <em>name</em>
     * properties is the preferred way of working with HyperGraph.
     * </p>
     * 
     * @param atomHandle A valid <code>HGPersistentHandle</code> of the atom being defined. If an atom
     * already exists with this handle, it will be replaced. This parameter cannot be <code>null</code>.
     * @param instance The handle of the atom's value. May be <code>null</code> in which case the default
     * HyperGraph <code>NullType</code> is used. 
     * @param outgoingSet If the atom is a link, this parameter specifies the set of atoms pointed to by the link.
     * If this parameter is <code>null</code> or of size 0, then the atom is not a link.
     */
    public void define(final HGPersistentHandle atomHandle, 
    				   final Object instance, 
    				   final HGHandle [] outgoingSet,
    				   final byte flags)
    {
    	getTransactionManager().transact(new Callable<Object>() 
    	{ public Object call() {
	    	// HGPersistentHandle [] layout = new HGPersistentHandle[outgoingSet == null ? 2 : 2 + outgoingSet.length];    	
	    	HGHandle typeHandle = null;
	    	if (instance == null)
	    		typeHandle = HGTypeSystem.NULLTYPE_PERSISTENT_HANDLE;
	    	else
	    		typeHandle = typeSystem.getTypeHandle(instance.getClass());
	    	if (typeHandle == null)
	    		throw new HGException("Could not find HyperGraph type for object of type " + instance.getClass());
	    	HGAtomType type = typeSystem.getType(typeHandle);
	    	HGPersistentHandle valueHandle = type.store(instance);
	    	define(atomHandle, typeHandle, valueHandle, outgoingSet);
	    	HyperGraph.this.atomAdded(atomHandle, instance, flags);
	    	return null;
    	}});
    }
    
    /**
     * <p>Delegate to <code>define(HGPersistentHandle, Object, HGHandle [], byte)</code> with the
     * flags parameter = 0.
     *  
     * @param atomHandle The handle of the atom to define.
     * @param instance The atom's runtime instance.
     * @param outgoingSet The target set when the atom is a link.
     */
    public void define(final HGPersistentHandle atomHandle, 
			   final Object instance, 
			   final HGHandle [] outgoingSet)
    {
    	define(atomHandle, instance, outgoingSet, (byte)0);
    }
    
    /**
     * <p>Return a <code>HGHandle</code> array of all <code>HGLink</code>s pointing 
     * to the atom referred by the passed in handle.</p>
     * 
     * @param handle The handle of the atom whose incidence set is desired.
     * @return An array of  <code>HGHandle</code>s of all the links pointing to this atom.
     * The returned array may have 0 elements, but it will never be <code>null</code>.
     */
    public HGHandle [] getIncidenceSet(HGHandle handle)
    {
        HGLiveHandle lHandle = null;
        HGPersistentHandle pHandle;
        
        if (handle instanceof HGPersistentHandle)
        {
            pHandle = (HGPersistentHandle)handle;
        	lHandle = cache.get(pHandle);            
        }
        else
        {
            lHandle = (HGLiveHandle)handle;
            pHandle = lHandle.getPersistentHandle();
        }
        
        HGHandle [] result = cache.getIncidenceSet(pHandle);
        
        if (result == null)
        {
            result = store.getIncidenceSet(pHandle);

            //
            // Update persistent handles with live handles
            //
            for (int i = 0; i < result.length; i++)
            {
                //
                // If the incidence link is loaded, use its live handle in the
                // incidence set.
                //
                HGLiveHandle liveLink = cache.get(result[i]);
                if (liveLink != null)
                {
                    result[i] = liveLink;
                    //
                    // If, in addition, the atom whose incidence set was just fetched
                    // is live, make sure the loaded link points to its live handle.
                    //
                    if (lHandle != null)
                    {
                        HGLink link = (HGLink)liveLink.getRef();
                        if (link != null) // make sure it's not nullified because of cache eviction
                        	updateLinkLiveHandle(link, lHandle);
                    }
                }
            }
            cache.incidenceSetRead(pHandle, result);            
        }
        return result;
    }
    
    public int getSystemFlags(HGHandle handle)
    {
    	if (handle instanceof HGLiveHandle)
    		if (handle instanceof HGManagedLiveHandle)
    			return ((HGManagedLiveHandle)handle).getFlags();
    		else
    			return 0;
    	else
    	{
    		AtomAttrib attribs = this.getAtomAttributes((HGPersistentHandle)handle);
    		if (attribs != null)
    			return attribs.flags;
    		else
    			return 0;
    	}
    }
    
    public void setSystemFlags(final HGHandle handle, final int flags)
    {
    	getTransactionManager().transact(new Callable<Object>() 
    	{ public Object call() {
	    	//
	    	// NOTE: there are several cases here. We may be switching from
	    	// default to non-default or vice-versa. We may be switching from
	    	// managed to non-managed or vice-versa. In the first situation, we
	    	// need to take care of adding/removing atom attributes. In all cases,
	    	// we have to adjust the live handle from/to managed/normal.
	    	//
	    	HGPersistentHandle pHandle = getPersistentHandle(handle);
	    	AtomAttrib attribs = HyperGraph.this.getAtomAttributes(pHandle);
	    	boolean managed = (flags & HGSystemFlags.MANAGED) != 0;
	    	boolean wasManaged = false;
	    	if (attribs != null)
	    	{
	    		wasManaged = (attribs.flags & HGSystemFlags.MANAGED) != 0; 
	    		if (flags == HGSystemFlags.DEFAULT)
	    		{
	    			HyperGraph.this.removeAtomAttributes(pHandle);
	    			attribs = null;
	    		}
	    		else
	    		{
	    			if (!wasManaged && managed)
	    			{
	    				attribs.lastAccessTime = System.currentTimeMillis();
	    				attribs.retrievalCount = 1;
	    			}    			
	    			attribs.flags = (byte)flags;
	    			HyperGraph.this.setAtomAttributes(pHandle, attribs);
	    		}
	    	}
	    	else if (flags != HGSystemFlags.DEFAULT)
	    	{
	    		attribs = new AtomAttrib();
	    		attribs.flags = (byte)flags;
	    		if (managed)
	    		{
	    			attribs.lastAccessTime = System.currentTimeMillis();
	    			attribs.retrievalCount = 1;
	    		}
	    		HyperGraph.this.setAtomAttributes(pHandle, attribs);
	    	}
	    	// else if we are trying to set the flags to default which they already are!
	    	else
	    	{
	    		return null;
	    	}
	    	
	    	HGLiveHandle lHandle = cache.get(pHandle);
	    	if (lHandle != null)
	    	{
	    		Object instance = lHandle.getRef();
	    		if (wasManaged && managed)
	    		{
	    			attribs.lastAccessTime = ((HGManagedLiveHandle)lHandle).getLastAccessTime();
	    			attribs.retrievalCount = ((HGManagedLiveHandle)lHandle).getRetrievalCount();
	    		}    		
	    		cache.remove(lHandle);
	    		if (instance != null)
	    		{
		    		if (managed)
		    			cache.atomRead(pHandle, instance, (byte)flags, 0 /* attribs.retrievalCount */, attribs.lastAccessTime);
		    		else
		    			cache.atomRead(pHandle, instance, (byte)flags);
	    		}
	    	}
	    	return null;
    	}});
    }
    
    /**
     * <p>Run a HyperGraphDB lookup query based on the specified condition.</p>
     * 
     * @param condition The <code>HGQueryCondition</code> constraining the returned
     * result set. It cannot be <code>null</code>.
     */
    public <T> HGSearchResult<T> find(HGQueryCondition condition)
    {
    	HGQuery query = HGQuery.make(this, condition);
        return query.execute();
    }
    
    /**
     * <p>Run a HyperGraphDB query based on the specified expression. For the 
     * syntax of the HyperGraphDB query language, please consult the reference
     * manual.
     * 
     * @param expression The query expression. Cannot be <code>null</code>
     * @return The <code>HGSearchResult</code> of the query.
     */
/*    public HGSearchResult find(String expression)
    {
    	HGQuery query = HGQuery.make(this, expression);
    	return query.execute();
    } */
    
    /**
     * <p>Create an index (if not already existing) along a specified dimension of 
     * composite HyperGraph type.</p>
     * 
     * <p>
     * All subsequent additions and removals of atoms of this type will trigger
     * updates of the newly created index. Note that if HyperGraph already contains
     * atoms of that type, they will be scanned to populate the index based on
     * the current data. Hence this operation may take some time to complete.
     * (<strong>NOTE:</strong> we don't have feedback mechanism yet for long
     * hypergraph operations, but once we do, such operations will be calling back
     * progress update listeners and the like.
     * </p>
     * 
     * @param typeHandle The handle of the type for which the index must be created.
     * @param dimensionPath A sequence of dimension names pointing to the nested type
     * dimension which must be indexed. If such a dimension cannot be navigated to
     * by following a composite type nesting, a <code>HGException</code> will be thrown.
     * @return <code>true</code> if  a new index was created and <code>false</code> otherwise.
     */
/*    public boolean createIndex(HGHandle typeHandle, String [] dimensionPath)
    {
    	ByPartIndexer indexer = new ByPartIndexer(typeHandle, dimensionPath);
    	return idx_manager.register(indexer);
//    	return idx_manager.createIndex(getPersistentHandle(typeHandle), dimensionPath);
    } */
    
    /**
     * <p>Return the index of a given type dimension (a.k.a. property). The index
     * must have been previsouly created with the <code>createIndex</code> method,
     * otherwise this method returns <code>null</code>.</p>
     *
     * @param The handle to the type.
     * @param A <code>String [] </code> representing the path to the type dimension.
     */
/*    public HGIndex getIndex(HGHandle typeHandle, String [] dimensionPath)
    {
    	return idx_manager.getIndex(getPersistentHandle(typeHandle), dimensionPath);
    } */
    
    /**
     * <p>Remove a previously created index of a given type dimension.</p>
     *
     * @param The handle to the type.
     * @param A <code>String [] </code> representing the path to the type dimension.
     */
/*    public void removeIndex(HGHandle typeHandle, String [] dimensionPath)
    {
    	idx_manager.removeIndex(getPersistentHandle(typeHandle), dimensionPath);
    } */
    
    /**
     * <p>
     * Return the <code>HGIndexManager</code> that is associated with this
     * HyperGraph instance. The index manager may be used to create indices
     * for specific atoms types. Such indices may result in quicker queries
     * at the expense of slower atom insertions. 
     * </p>
     */
    public HGIndexManager getIndexManager()
    {
    	return idx_manager;
    }
    
    // ------------------------------------------------------------------------
    // PRIVATE METHOD SECTION
    // ------------------------------------------------------------------------
    
    private HGLiveHandle addNode(final Object payload, final HGHandle typeHandle, final byte flags)
    {
    	return getTransactionManager().transact(new Callable<HGLiveHandle>() 
    	{ public HGLiveHandle call() {
	    	HGAtomType type = typeSystem.getType(typeHandle);
	    	HGPersistentHandle pTypeHandle = getPersistentHandle(typeHandle);    	
	        HGPersistentHandle valueHandle = type.store(payload);  
	
	        HGPersistentHandle [] layout = new HGPersistentHandle[2];            
	        layout[0] = pTypeHandle;
	        layout[1] = valueHandle;
	        final HGLiveHandle lHandle = atomAdded(store.store(layout), payload, flags);
	        indexByType.addEntry(pTypeHandle, lHandle.getPersistentHandle());
	        indexByValue.addEntry(valueHandle, lHandle.getPersistentHandle());
	        idx_manager.maybeIndex(pTypeHandle, type, lHandle.getPersistentHandle(), payload);	        
	        return lHandle;    
    	} });
    	
    }
    
    /**
     * Add a link to the store: because we can have a wrapped HGValueLink, we
     * pass the data as two parameters - payload and outgoingSet. The actual HG
     * atom is always the outgoingSet parameter though.
     */
    private HGLiveHandle addLink(final Object payload, 
    							 final HGHandle typeHandle, 
    							 final HGLink outgoingSet, 
    							 final byte flags)
    {
    	return getTransactionManager().transact(new Callable<HGLiveHandle>() 
        { public HGLiveHandle call() {
	    	HGAtomType type = typeSystem.getType(typeHandle);
	    	HGPersistentHandle pTypeHandle = getPersistentHandle(typeHandle);
	        HGPersistentHandle valueHandle = type.store(payload);            
	        
	        //
	        // Prepare link layout.
	        //            
	        HGPersistentHandle [] layout = new HGPersistentHandle[2 + outgoingSet.getArity()];            
	        layout[0] = pTypeHandle;
	        layout[1] = valueHandle;
	        for (int i = 0; i < outgoingSet.getArity(); i++)
	            layout[i + 2] = getPersistentHandle(outgoingSet.getTargetAt(i));
	        
	        //
	        // Store in database.
	        //
	        HGLiveHandle lHandle = atomAdded(store.store(layout), outgoingSet, flags);
	        
	        //
	        // Update the incidence sets of all its targets.
	        //
	        updateTargetsIncidenceSets(lHandle);
	        
	        indexByType.addEntry(pTypeHandle, lHandle.getPersistentHandle());
	        indexByValue.addEntry(valueHandle, lHandle.getPersistentHandle());
	        idx_manager.maybeIndex(pTypeHandle, type, lHandle.getPersistentHandle(), payload);	        
	        return lHandle;
    	}});
    }
    
    private HGLiveHandle atomAdded(HGPersistentHandle pHandle, Object instance, byte flags)
    {
        if ( (flags & HGSystemFlags.MANAGED) != 0)
        {
        	AtomAttrib attribs = new AtomAttrib();
        	attribs.flags = flags;
        	attribs.retrievalCount = 1;
        	attribs.lastAccessTime = System.currentTimeMillis();
        	setAtomAttributes(pHandle, attribs);        	
        	return cache.atomRead(pHandle, 
        			 		      instance, 
        						  attribs.flags, 
        						  attribs.retrievalCount, 
        						  attribs.lastAccessTime);
        }        
        else
        {
        	if (flags != 0)
        	{
        		AtomAttrib attribs = new AtomAttrib();
        		attribs.flags = flags;
        		setAtomAttributes(pHandle, attribs);
        	}
        	return cache.atomRead(pHandle, instance, flags);
        }
    }
    
    /**
     * Loads an atom from storage. Returns a pair of (live handle, run-time instance) because the instance may very
     * well get GC-ed before we even have the change to return it in the 'get' method above!
     *  
     * @param persistentHandle
     * @param liveHandle
     * @return
     */
    private Pair<HGLiveHandle, Object> loadAtom(final HGPersistentHandle persistentHandle,  final HGLiveHandle liveHandle)    
    {
    	return getTransactionManager().transact(new Callable<Pair<HGLiveHandle, Object>>() 
 	    { public Pair<HGLiveHandle, Object> call() {
	        Object instance;        
	        HGPersistentHandle [] link = store.getLink(persistentHandle);
	        
	        if (link == null)
	        {
	        	return null;
	        }
	        
	        if (link.length < 2)
	            throw new HGException("The persistent handle " + persistentHandle + 
	            					  " doesn't refer to a HyperGraph atom.");
	        
	        HGPersistentHandle typeHandle = link[0];
	        HGPersistentHandle valueHandle = link[1];
	            
	        if (typeHandle.equals(HGTypeSystem.TOP_PERSISTENT_HANDLE))
	        {
	        	HGLiveHandle result = typeSystem.loadPredefinedType(persistentHandle); 
	        	return new Pair<HGLiveHandle, Object>(result, result.getRef());
	        }
	        
	        IncidenceSetRef isref = new IncidenceSetRef(persistentHandle, HyperGraph.this);
	        
	        HGAtomType type = typeSystem.getType(typeHandle);
	        TypeUtils.initiateAtomConstruction(HyperGraph.this, valueHandle);
	        if (link.length == 2)	        	
	            instance = type.make(valueHandle, EMTPY_HANDLE_SET_REF, isref);
	        else
	        {
	            //
	            // If the atom is a link, update all targets with available
	            // live handles.
	            //
	            HGHandle [] targets = new HGHandle[link.length - 2];
	            for (int i = 2; i < link.length; i++)
	            {
	                HGPersistentHandle pHandle = link[i];
	                HGLiveHandle lHandle = (HGLiveHandle)cache.get(pHandle);
	                if (lHandle != null)
	                    targets[i-2] = lHandle;
	                else
	                    targets[i-2] = pHandle;
	            }
	            instance = type.make(valueHandle, 
	            					 new ReadyRef<HGHandle[]>(targets), 
	            					 isref);
	            
	            //
	            // If the concrete result instance is not a link, then it has
	            // been embedded into a value link.
	            //
	            if (! (instance instanceof HGLink))
	                instance = new HGValueLink(instance, targets);
	        }
	        TypeUtils.atomConstructionComplete(HyperGraph.this, valueHandle);
	        HGLiveHandle result = null;
	        if (liveHandle == null)
	        {
	        	AtomAttrib attribs = getAtomAttributes(persistentHandle);
	        	if (attribs != null)
	        		if ( (attribs.flags & HGSystemFlags.MANAGED) != 0)
		        		result = cache.atomRead(persistentHandle, 
		        						        instance, 
		        								attribs.flags, 
		        								1, // don't disturb the cache's own counting and importance calculation... 
		        								attribs.lastAccessTime);
	        		else
	        			result = cache.atomRead(persistentHandle, instance, attribs.flags);        			
	        	else
	        		result = cache.atomRead(persistentHandle, instance, (byte)0);
	        }
	        else
	        {
	        	result = liveHandle;
	        	cache.atomRefresh(result, instance);
	        }
	        if (instance instanceof HGAtomType)
	        	instance = typeSystem.loadedType(result, (HGAtomType)instance, true);
	    	if (instance instanceof HGGraphHolder)
	    		((HGGraphHolder)instance).setHyperGraph(HyperGraph.this);
	        eventManager.dispatch(HyperGraph.this, new HGAtomLoadedEvent(result, instance));  
	        return new Pair<HGLiveHandle, Object>(result, instance);
    	}});
    }
    
    private void unloadAtom(final HGLiveHandle lHandle, final Object instance)
    {
    	try
    	{
	    	getTransactionManager().transact(new Callable<Object>() 
	  	    { public Object call() {
		    	if ((lHandle.getFlags() & HGSystemFlags.MUTABLE) != 0)
		    	{
		    		//TODO: Maybe this should be done somewhere else or differently...
		    		//in atomAdded() attribs are added only for MANAGED flag
		    		AtomAttrib attrib = getAtomAttributes(lHandle.getPersistentHandle());
		    		if(attrib == null){
		    			attrib = new AtomAttrib();
		    			attrib.flags = lHandle.getFlags();
		    		   setAtomAttributes(lHandle.getPersistentHandle(), attrib);
		    		}
		    		//
		    		// We don't explicitly track what has changed in atom. So
		    		// we need to save its "whole" value. Because, the replace
		    		// operation is too general and it may interact with the cache
		    		// in complex ways, while this method may be called during cache
		    		// cleanup, we can't use 'replace'. We need a separate version
		    		// that is careful not to use the cache.
		    		//
		    		 // rawSave(lHandle.getPersistentHandle(), instance);
		    		replace(lHandle, instance);
		    	}
		    	if ((lHandle.getFlags() & HGSystemFlags.MANAGED) != 0)
		    	{
		    		HGManagedLiveHandle mHandle = (HGManagedLiveHandle)lHandle;
		    		AtomAttrib attrib = getAtomAttributes(mHandle.getPersistentHandle());
		    		attrib.flags = mHandle.getFlags();
		    		attrib.retrievalCount += mHandle.getRetrievalCount();
		    		attrib.lastAccessTime = Math.max(mHandle.getLastAccessTime(), attrib.lastAccessTime);
		    		setAtomAttributes(lHandle.getPersistentHandle(), attrib);
		    	}
		    	return null;
	    	}});
    	}
    	catch (HGException ex)
    	{
    		throw new HGException("Problem while unloading atom " + 
					  			  instance + " of type " + instance.getClass().getName() + " " + ex.getMessage(),
					  			  ex);
    	}
    	catch (Throwable t)
    	{
    		throw new HGException("Problem while unloading atom " + 
		  			  instance + " of type " + instance.getClass().getName(),
		  			  t);
    	}    	
    }
    
    /**
     * Update a link to point to a "live" target instead of holding a 
     * persistent handle. This is slightly inefficient as it needs
     * to loop through all targets of the link. It is here perhaps that
     * a distinction b/w ordered and unordered links might become useful
     * for efficiency purposes: an unordered link would be able to use
     * a hash lookup on its handles to find the one that needs to be updated,
     * instead of a linear traversal.
     */
    private void updateLinkLiveHandle(HGLink link, HGLiveHandle lHandle)
    {
        int arity = link.getArity();
        for (int i = 0; i < arity; i++)
        {
            HGHandle current = link.getTargetAt(i);
            if (current == lHandle)
                return;
            else if (current.equals(lHandle.getPersistentHandle()))
            {
                link.notifyTargetHandleUpdate(i, lHandle);
                return;
            }
        }
    }
        
    private void updateTargetsIncidenceSets(HGLiveHandle newLink)
    {
        HGLink link = (HGLink)get(newLink); 
        for (int i = 0; i < link.getArity(); i++)
        {
            HGPersistentHandle targetHandle = getPersistentHandle(link.getTargetAt(i)); 
            HGHandle [] targetIncidenceSet = cache.getIncidenceSet(targetHandle);
            if (targetIncidenceSet != null)
            {
                HGHandle [] newIncidenceSet = new HGHandle[targetIncidenceSet.length + 1];
                System.arraycopy(targetIncidenceSet, 0, newIncidenceSet, 0, targetIncidenceSet.length);
                newIncidenceSet[targetIncidenceSet.length] = newLink;
            }
            store.addIncidenceLink(targetHandle, newLink.getPersistentHandle());
        }                   
    }

    private void targetRemoved(HGHandle linkHandle, HGHandle target)
    {
    	HGLink l = (HGLink)get(linkHandle);
    	int pos = -1;
    	for (int i = 0; i < l.getArity(); i++)
    		if (target.equals(l.getTargetAt(i)))
    		{
    			pos = i;
    			break;
    		}
    	if (pos > -1)
    	{
    		l.notifyTargetRemoved(pos);
    		replace(linkHandle, l);
    	}
    }
    
    /**
     * Remove a link from the incidence set of a given atom. If the link is
     * not part of the incidence set of the <code>targetAtom</code>, nothing
     * is done.
     * 
     * @param targetAtom The handle of the atom whose incidence set need modification.
     * @param incidentLiveLink The <code>LiveHandle</code> of the link to be removed
     * or <code>null</code> if the live handle is not available.
     * @param incidentLink The <code>HGPersistentHandle</code> of the link to be
     * removed - cannot be <code>null</code>.
     */
    private void removeFromIncidenceSet(HGPersistentHandle targetAtom,
    									HGLiveHandle incidentLiveLink,
    									HGPersistentHandle incidentLink)
    {
        store.removeIncidenceLink(targetAtom, incidentLink);
        
        //
        // Remove from cached incidence set, if present.
        //
        HGHandle [] targetIncidenceSet = cache.getIncidenceSet(targetAtom);
        if (targetIncidenceSet != null && targetIncidenceSet.length > 0)
        {
            HGHandle [] newTargetIncidenceSet = new HGHandle[targetIncidenceSet.length - 1];
            int k = 0;
            for (int j = 0; j < targetIncidenceSet.length; j++)
                if (incidentLiveLink != null && incidentLiveLink == targetIncidenceSet[j] ||
                	incidentLink.equals(targetIncidenceSet[j]))
                    continue;
                else
                    newTargetIncidenceSet[k++] = targetIncidenceSet[j];
            cache.incidenceSetRead(targetAtom, newTargetIncidenceSet);
        }   	
    }

    /**
     * Save the run-time value of an atom back to the database store, without
     * disrupting the cache. It is assumed 
     * 
     * @param handle The persistent handle of the atom.
     * @param instance The run-time value of the atom.
     */
    private void rawSave(HGPersistentHandle handle, Object instance)
    {
		HGPersistentHandle [] layout = store.getLink(handle);
		if (layout == null)
			throw new HGException("Can't unload atom with handle " + 
							  	  handle + 
							  	  " it's even in the DB anymore.");
		HGAtomType type = (HGAtomType)rawGet(layout[0]);
		HGAtomType oldTypeInstance = null;
		if (instance instanceof HGAtomType)
			oldTypeInstance = (HGAtomType)rawMake(layout, type, handle);
		type.release(layout[1]);
		indexByValue.removeEntry(layout[1], handle);
		layout[1] = type.store(instance);
		indexByValue.addEntry(layout[1], handle);		
		if (oldTypeInstance != null)
		{
			HGSearchResult<HGPersistentHandle> rs = indexByType.find(handle);
			try
			{
				while (rs.hasNext())
				{
					HGPersistentHandle h = rs.next();
					HGPersistentHandle [] L = store.getLink(h);
					if (h == null)
						throw new HGException("Can't load layout for atom " + h);
					rawSave(h, rawMake(L, oldTypeInstance, h));
				}
			}
			finally
			{				
				rs.close();
			}
		}
    }
    
    /**
     * Make a run-time instance given a layout and a type. Ignore caching,
     * incidence set management etc. 
     * 
     * @param layout
     * @param type
     * @return
     */
    private Object rawMake(HGPersistentHandle [] layout, HGAtomType type, HGPersistentHandle atomHandle)
    {
    	HGPersistentHandle [] targetSet = EMPTY_PERSISTENT_HANDLE_SET;
    	if (layout.length > 2)
    	{
    		targetSet = new HGPersistentHandle[layout.length - 2];
    		for (int i = 2; i < layout.length; i++)
    			targetSet[i - 2] = layout[i];
    	}
    	TypeUtils.initiateAtomConstruction(HyperGraph.this, layout[1]);
    	Object result = type.make(layout[1], 
    							  new ReadyRef<HGHandle[]>(targetSet), 
    							  new IncidenceSetRef(atomHandle, this));
    	TypeUtils.atomConstructionComplete(HyperGraph.this, layout[1]);
        if (targetSet.length > 0 && ! (result instanceof HGLink))
            result = new HGValueLink(result, targetSet);
        if (result instanceof HGAtomType)
        	result = typeSystem.loadedType(new DefaultLiveHandle(result, 
        														 atomHandle, 
        														 (byte)0), 
        								   (HGAtomType)result, 
        								   false);
        return result;    	
    }

    /**
     * Retrieve an atom without modifying the cache.
     * 
     * @param h
     * @return
     */
    private Object rawGet(HGHandle h)
    {
    	HGPersistentHandle ph = null;
    	if (h instanceof HGLiveHandle)
    	{
    		Object x = ((HGLiveHandle)h).getRef();
    		if (x != null)
    			return x;
    		else
    			ph = ((HGLiveHandle)h).getPersistentHandle();
    	}
    	else
    		ph = (HGPersistentHandle)h;
    	HGPersistentHandle [] layout = store.getLink(ph);
    	if (layout == null)
    		return null;
    	else if (HGTypeSystem.TOP_PERSISTENT_HANDLE.equals(layout[0]))
    		return cache.get(ph).getRef();
    	else 
    	{
    		HGAtomType type = (HGAtomType)rawGet(layout[0]);
    		return rawMake(layout, type, ph);
    	}
    }
    
    /**
     * Replace an atom with a new value. Recursively replace the values of type atoms.
     * 
     * @param lHandle The live handle of the atom, if available, can be null...
     * @param pHandle The persistent handle of the atom, can't be null
     * @param atom The new value of the atom
     * @param typeHandle The type of the new value
     */
    private void replaceInternal(final HGLiveHandle lHandle, 
    							 final HGPersistentHandle pHandle, 
    							 final Object atom, 
    							 final HGHandle typeHandle)
    {
    	getTransactionManager().transact(new Callable<Object>() 
  	    { public Object call() {
	        Object newValue = atom;
	        if (atom instanceof HGValueLink)
	        	newValue = ((HGValueLink)atom).getValue();
	
	        HGPersistentHandle [] layout = store.getLink(pHandle);
	        HGPersistentHandle oldValueHandle = layout[1];        
	        HGPersistentHandle oldTypeHandle = layout[0];
	        HGAtomType oldType = (HGAtomType)get(oldTypeHandle);
	        HGAtomType type = (HGAtomType)get(typeHandle);        
	        
	    	Object oldValue;
	        if (lHandle != null && (oldValue = lHandle.getRef()) != null)
	        	;
	        else
	        	oldValue = rawMake(layout, oldType, pHandle); //rawMake will just construct the instance, without adding to cache
	        
	    	if (oldValue instanceof HGValueLink)
	    		oldValue = ((HGValueLink)oldValue).getValue();
	        
	    	//
	    	// If the atom is a type, we need to morph all its values to the new
	    	// type. This is done simply by recursively replacing instances
	    	// based on the old type atom with instanaces of the new type. 
	    	//    	
	    	if (oldValue instanceof HGAtomType)
	    	{
	    		HGSearchResult<HGPersistentHandle> rs = null;
	    		try
	    		{
	    			rs = indexByType.find(pHandle);
	    		
		    		if (rs.hasNext() && ! (newValue instanceof HGAtomType))
		    			throw new HGException("Attempt to replace a type atom " + pHandle + 
		    					" with a non-empty instance set by an atom that is not a HyperGraph type.");
		    		HGAtomType oldTypeValue = (HGAtomType)oldValue;
		    		HGAtomType newTypeValue = (HGAtomType)newValue;
		    		while (rs.hasNext())
		    			morph((HGPersistentHandle)rs.next(), oldTypeValue, newTypeValue);
	    		}
	    		finally
	    		{
	    			rs.close();
	    		}
	    	}
	    	
	        if (!oldTypeHandle.equals(typeHandle))
	        {        	
	        	indexByType.removeEntry(getPersistentHandle(oldTypeHandle), pHandle);
	        	indexByType.addEntry(getPersistentHandle(typeHandle), pHandle);
	        }
	        
	        TypeUtils.releaseValue(HyperGraph.this, layout[1]);
	        oldType.release(layout[1]);
	        layout[1] = type.store(newValue);
	        layout[0] = getPersistentHandle(typeHandle);
	    	indexByValue.removeEntry(oldValueHandle, pHandle);
	    	indexByValue.addEntry(layout[1], pHandle);
	    	
	        HGPersistentHandle [] newLayout;
	        
	    	if (atom instanceof HGLink)
	    	{
	    		HGLink newLink = (HGLink)atom;
	    		newLayout = new HGPersistentHandle[newLink.getArity() + 2];
	    		
				// If we are replacing a link by a link. We need to compute the
				// delta of the target sets and remove the link from incidence
				// sets where it no longer belongs.    		
	    		HashMap<HGPersistentHandle, Boolean> newTargets = new HashMap<HGPersistentHandle, Boolean>();
				for (int i = 0; i < newLink.getArity(); i++)
				{
					HGPersistentHandle target = getPersistentHandle(newLink.getTargetAt(i)); 
					newLayout[2 + i] = target;
					if (layout.length > 2) 
						newTargets.put(target, Boolean.TRUE);
				}    		
				for (int i = 2; i < layout.length; i++)
					if (newTargets.get(layout[i]) != null)
						removeFromIncidenceSet(layout[i], lHandle, pHandle);
	    	}
	    	else 
	        {
	    		newLayout = new HGPersistentHandle[2];
				for (int i = 2; i < layout.length; i++)
					removeFromIncidenceSet(layout[i], lHandle, pHandle);
	        }
			newLayout[0] = layout[0];
			newLayout[1] = layout[1];    	
	    	store.store(pHandle, newLayout);    		
	    	
	    	if (lHandle != null)
	    		cache.atomRefresh(lHandle, atom);
	    	return null;
    	}});
    }
    
    /**
     * Change the type value of an atom value. It is assumed that oldType and newType keep
     * the same handle. This is used when we are replacing the value of a type atom with another
     * value (which must be a compatible type value). Morphing can potentially lead to a
     * different run-time instance of the atom. So, if the atom is "alive", we replace the
     * run-time instance.
     *   
     * @param instanceHandle The atom 
     * @param oldType the old type instance
     * @param newType the new type instance
     */
    private void morph(HGPersistentHandle instanceHandle, HGAtomType oldType, HGAtomType newType)
    {
    	HGPersistentHandle [] layout = store.getLink(instanceHandle);
		Object oldInstance = rawMake(layout, oldType, instanceHandle);
		TypeUtils.releaseValue(this, layout[1]);
		oldType.release(layout[1]);
		indexByValue.removeEntry(layout[1], instanceHandle);
		layout[1] = newType.store(oldInstance);
		indexByValue.addEntry(layout[1], instanceHandle);
		Object newInstance = rawMake(layout, newType, instanceHandle);
		
		HGLiveHandle instanceLiveHandle = cache.get(instanceHandle);
		if (instanceLiveHandle != null && instanceLiveHandle.getRef() != null)
			cache.atomRefresh(instanceLiveHandle, newInstance);
		
		if (oldInstance instanceof HGAtomType)
		{
    		HGSearchResult<HGPersistentHandle> rs = null;
    		
    		try
    		{
	    		rs = indexByType.find(instanceHandle);
	    		if (rs.hasNext() && ! (newInstance instanceof HGAtomType))
					throw new HGException("Cannot replace value of atom " + instanceHandle + 
							" which was a type with something that is not a type");
	    		oldType = (HGAtomType)oldInstance;
	    		newType = (HGAtomType)newInstance;
	    		while (rs.hasNext())
	    		{
	    			morph((HGPersistentHandle)rs.next(), oldType, newType);
	    		}
    		}
    		finally
    		{
    			if (rs != null) rs.close();
    		}
		}
		// if newInstance is a type, but not oldInstance, we're ok...
	}
    
    @SuppressWarnings("unchecked")
    private void initAtomManagement()
    {
    	//
    	// Init HGStats atom.
    	//
    	HGSearchResult<HGPersistentHandle> rs = null;
    	
    	try
    	{
	    	rs = find(new AtomTypeCondition(typeSystem.getTypeHandle(HGStats.class)));
	    	if (rs.hasNext())
	    	{
	    		statsHandle = rs.next();
	    		stats = (HGStats)get(statsHandle);
	    	}
	    	else
	    	{
	    		statsHandle = add(stats);
	    	}
    	}
    	catch (HGException ex)
    	{
    		throw ex;
    	}
    	catch (Throwable t)
    	{
    		throw new HGException(t);
    	}
    	finally
    	{
    		if (rs != null) rs.close();
    	}
    	
    	eventManager.addListener(
    			HGAtomEvictEvent.class,
    			new HGListener<HGAtomEvictEvent>()
    			{
    				public HGListener.Result handle(HyperGraph hg, HGAtomEvictEvent ev)
    				{
    					unloadAtom((HGLiveHandle)ev.getAtomHandle(), ev.getInstance());
    					return Result.ok;
    				}
    			}
    			);    	
    }
    
    private void loadListeners()
    {
    	HGSearchResult rs = null;
    	try
    	{
    		rs = find(new AtomTypeCondition(typeSystem.getTypeHandle(HGListenerAtom.class)));
    		while (rs.hasNext())
    		{
    			HGListenerAtom listenerAtom = (HGListenerAtom)get((HGHandle)rs.next());
    			Class eventClass, listenerClass;
    			try
    			{
    				eventClass = Class.forName(listenerAtom.getEventClassName());
    				listenerClass = Class.forName(listenerAtom.getListenerClassName());
    				eventManager.addListener(eventClass, (HGListener)listenerClass.newInstance());
    			}
    			catch (Throwable t)
    			{
    				logger.exception(t);
    			}
    		}
    	}
    	catch (Throwable t)
    	{
    		throw new HGException(t);
    	}
    	finally
    	{
    		if (rs != null) rs.close();    		
    	}    	
    }
    
    private AtomAttrib getAtomAttributes(HGPersistentHandle handle)
    {
    	return systemAttributesDB.findFirst(handle);    	
    }
    
    private void setAtomAttributes(HGPersistentHandle handle, AtomAttrib attribs)
    {
    	systemAttributesDB.removeAllEntries(handle);
    	systemAttributesDB.addEntry(handle, attribs);
    }
    
    private void removeAtomAttributes(HGPersistentHandle handle)
    {
    	systemAttributesDB.removeAllEntries(handle);
    }
}