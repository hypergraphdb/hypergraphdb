/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.cache.HGCache;
import org.hypergraphdb.cache.LRUCache;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.handle.HGManagedLiveHandle;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.type.*;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.atom.HGStats;
import org.hypergraphdb.maintenance.MaintenanceException;
import org.hypergraphdb.maintenance.MaintenanceOperation;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.event.*;
import org.hypergraphdb.transaction.*;
import org.hypergraphdb.util.HGDatabaseVersionFile;
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
 * Because HyperGraphDB is primarily an embedded database, it does not rely on a client-server
 * model and therefore there is no notion of a connection. The database is accessed by
 * instantiating an instance of this class and then using its methods. An instance of this
 * class is essentially a connection to the database and it must be always properly closed
 * before application shutdown. To close the database, call the <code>close</code> method. 
 * When the application exits abruptly, without having a chance to close, it may fail to
 * open subsequently without running a recovery process. The recovery process depends on the
 * underlying storage implementation. The default storage implementation performs a light recovery
 * process every time you open a database. If a full recovery is needed, please consult the 
 * documentation of the storage implementation.
 * </p>  
 *   
 * <p>
 * This class encapsulates and offers access to all important HyperGraphDB top-level objects such as
 * its associated {@link HGTransactionManager}, {@link HGTypeSystem},
 * {@link HGEventManager} and the low-level {@link HGStore}.
 * </p>
 * 
 * <p>
 * Each datum in a HyperGraph database is called an <code>atom</code>. Atoms are either
 * arbitrary plain objects or instances of {@link HGLink}. Using this class, you may:
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
 * {@link HGIndexManager} associated with a HyperGraph instance. Call <code>getIndexManager</code>
 * to get the index manager.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public /*final*/ class HyperGraph implements HyperNode
{
    public static final HGHandle [] EMPTY_HANDLE_SET = new HGHandle[0];
    public static final LazyRef<HGHandle[]> EMPTY_HANDLE_SET_REF = new ReadyRef<HGHandle[]>(new HGHandle[0]);
    public static final HGPersistentHandle [] EMPTY_PERSISTENT_HANDLE_SET = new HGPersistentHandle[0];
    
    /**
     * An object ID for locking the incidence set cache within a DB transaction.
     */
    //private static final byte [] INCIDENCE_CACHE_ID = HGHandleFactory.makeHandle("128d0be0-b062-11dd-b416-0002a5d5c51b").toByteArray();
    
    /**
     * The resource name of the default types configuration file.
     */
//    public static final String TYPES_CONFIG_FILE = "/org/hypergraphdb/types";
    /**
     * The name of the main by-type atom index.
     */
    public static final String TYPES_INDEX_NAME = "HGATOMTYPE";
    /**
     * The name of the main by-value atom index.
     */
    public static final String VALUES_INDEX_NAME = "HGATOMVALUE";
    /**
     * The name of the atom attributes DB.
     */
    public static final String SA_DB_NAME = "HGSYSATTRIBS";
    
    /**
     * The location (full directory path on disk) for this HyperGraph database. 
     */
    private String location = null;
    
    /**
     * Is a database currently open?
     */
    private volatile boolean is_open = false;
    
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
    HGIndex<HGPersistentHandle, HGAtomAttrib> systemAttributesDB = null;
    
    HGHandle statsHandle = null;
    HGStats stats = new HGStats();
    HGConfiguration config = new HGConfiguration();
    
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
     * <p>Return the set of configuration parameters for this HyperGraphDB instance.</p>
     */
    public HGConfiguration getConfig()
	{
		return config;
	}

    /**
     * <p>Specify configuration parameters for this HyperGraphDB instance. If the
     * instance is already opened, most parameters will not take effect.
     * </p>
     * 
     * @param config A <code>HGConfiguration</code> holding the parameters. Must not be
     * <code>null</code> - it is ignored if it is.
     */
	public void setConfig(HGConfiguration config)
	{
		if (config == null)
			return;
		this.config = config;		
	}

	/**
	 * <p>Return the {@link HGHandleFactory} implementation associated with this
	 * HyperGraph instance. The handle factory is responsible for managing the 
	 * representation of persistent handles - generating new ones, converting to and
	 * from <code>byte[]</code> as well as the several predefined handles with special
	 * semantics.</p>
	 */
	public HGHandleFactory getHandleFactory()
	{
	    return config.getHandleFactory();
	}
	
	/**
	 * <p>Return the {@link HGLogger} associated with this graph.</p>
	 */
	public HGLogger getLogger()
	{
		return logger;
	}
	
	/**
	 * <p>Execute all currently scheduled maintenance operations. Note that calling this
	 * method can potentially take a long time. Also, it is imperative that no other
	 * thread accesses this HyperGraphDB instance while the maintenance operations are
	 * being executed.
	 * </p>
	 * <p>
	 * This method is invoked by default when a HyperGraphDB instance is open, unless
	 * the <code>HGConfiguration</code> specifies that maintenance operations must be
	 * canceled or skipped. The method is made public for convenience to applications that
	 * "know" what they are doing - the same effect can be achieved by closing and re-opening
	 * the HyperGraphDB instance, but with the side effect of loosing all cached information. 
	 * </p>
	 */
	public void runMaintenance()
	{
		List<MaintenanceOperation> L = HGQuery.hg.getAll(this, HGQuery.hg.typePlus(MaintenanceOperation.class));
		for (MaintenanceOperation op : L)
			try 
			{ 
				op.execute(this);
				remove(getHandle(op));
			}
			catch (MaintenanceException ex)
			{
				ex.printStackTrace(System.err);
				if (ex.isFatal())
					break;
			}
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
	        store = new HGStore(location, config);
	        store.getTransactionManager().setHyperGraph(this);
	        eventManager = config.getEventManager();	 
	        eventManager.setHyperGraph(this);
	        cache = config.getCacheImplementation();
	        cache.setHyperGraph(this);
	        HGCache<HGPersistentHandle, IncidenceSet> incidenceCache = 
	        	new LRUCache<HGPersistentHandle, IncidenceSet>(0.9f, 0.3f);
	        ((LRUCache<HGPersistentHandle, IncidenceSet>)incidenceCache).setLockImplementation(
                    new ReentrantReadWriteLock()	                                                                                           
	        		/* new HGLock(this, INCIDENCE_CACHE_ID) */);
	        	// new SimpleCache<HGPersistentHandle, IncidenceSet>();
	        incidenceCache.setResolver(new ISRefResolver(this));
	        cache.setIncidenceCache(incidenceCache);
	        
	        typeSystem = new HGTypeSystem(this);    
	        
	        //
	        // Make sure system indices are created.
	        //
	        indexByType = store.getIndex(TYPES_INDEX_NAME, 
	                                     BAtoHandle.getInstance(this.getHandleFactory()), 
	                                     BAtoHandle.getInstance(this.getHandleFactory()), 
	                                     null,
	                                     null,
	                                     true);	        						     	        
	        indexByValue = store.getIndex(VALUES_INDEX_NAME, 
	                                      BAtoHandle.getInstance(this.getHandleFactory()), 
	                                      BAtoHandle.getInstance(this.getHandleFactory()), 
	                                      null,
	                                      null,
	                                      true);	        
	        if (config.isUseSystemAtomAttributes())
    	        systemAttributesDB = store.getIndex(SA_DB_NAME, 
    	                                            BAtoHandle.getInstance(this.getHandleFactory()), 
    	                                            HGAtomAttrib.baConverter, 
    	                                            null,
    	                                            null,
    	                                            true);
	        
	        idx_manager = new HGIndexManager(this);
	        
	        //
	        // Now, bootstrap the type system.
	        //
	        getTransactionManager().beginTransaction(HGTransactionConfig.DEFAULT);
	        typeSystem.bootstrap(config.getTypeConfiguration());                 
	        getTransactionManager().endTransaction(true);
            
	        idx_manager.loadIndexers();
    		            
	        // Initialize atom access statistics, purging and the like. 
	        initAtomManagement();    
            	        
	        // Load all listeners stored in this HyperGraph as HGListenerAtoms
	        getTransactionManager().beginTransaction(HGTransactionConfig.DEFAULT);
	        loadListeners();
	        getTransactionManager().endTransaction(true);
	        
	        // This is kind of completing the type system bootstrap process, otherwise
	        // there's a circular dependency between the initialization of the index manager
	        // and the JavaObjectMapper (which has an index on the HGSerializable atoms). Another
	        // option to avoid this typical bootstrapping circularity is the implement the JavaObjectMapper
	        // to directly work with the HGStore instead of relying on the index manager. In any case,
	        // it all remains an implementation detail.
//	        typeSystem.getJavaTypeFactory().initNonDefaultMappers();
	        
	        if (config == null || !config.getSkipOpenedEvent())
	            eventManager.dispatch(this, new HGOpenedEvent());	
	        
	        HGDatabaseVersionFile versionFile = HGEnvironment.getVersions(location);
	        if (versionFile.getVersion("hgdb") == null)
	        	versionFile.setVersion("hgdb", "1.3");
	        if (config != null)
	        {
	        	if (config.getCancelMaintenance())
	        	{
	        		List<HGHandle> L = HGQuery.hg.findAll(this, HGQuery.hg.typePlus(MaintenanceOperation.class));
	        		for (HGHandle x : L)
	        			remove(x);
	        	}
	        	else if (!config.getSkipMaintenance())
	        		runMaintenance();
	        }
	        	        
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
    	// Acquiring the HGEnvironment.class lock is consistent with the fact that closing
    	// a graph is changing the global HGEnviornment. Previously we had a separate
    	// internal lock just for this graph object, but that led to deadlocks due
    	// to the cache also accessing the memory hook of the environment.
    	synchronized (HGEnvironment.class) 
    	{
	        if (!is_open)
	            return;
	        ArrayList<Throwable> problems = new ArrayList<Throwable>();
	        try { eventManager.dispatch(this, new HGClosingEvent()); } catch (Throwable t) { problems.add(t); }
	    	try { replace(statsHandle, stats);  					 } catch (Throwable t) { problems.add(t); }     
	        try { cache.close(); 									 } catch (Throwable t) { problems.add(t); }        
	    	try { idx_manager.close();								 } catch (Throwable t) { problems.add(t); }
	    	try { eventManager.clear();								 } catch (Throwable t) { problems.add(t); }
	        try { store.close();                                     } catch (Throwable t) { problems.add(t); }
	        is_open = false;
	        for (Throwable t : problems)
	        {
	        	System.err.println("Problem during HyperGraph close, stack trace of exception follows:");
	        	t.printStackTrace(System.err);
	        }
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
     * <p>Return the atom cache associated with this HyperGraph instance.</p>
     */
    public HGAtomCache getCache()
    {
        return cache;
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
     * with atom handles, one needn't worry whether they are in-memory or not. However,
     * some low-level APIs dealing with permanent storage explicitly require 
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
            return ((HGLiveHandle)handle).getPersistent();
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
    		return cache.get(handle.getPersistent()) != null;
    	else
    		return ((HGLiveHandle)handle).getRef() != null;
    }
    
    /**
     * <p>Return <code>true</code> if a given is currently frozen in the cache
     * and <code>false</code> otherwise. Frozen atoms are guaranteed to NOT be evicted
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
     * Freeze an atom into the HyperGraph cache.Frozen atoms are guaranteed to NOT be evicted
     * from the cache. If the atom is not already currently loaded, it will be loaded. If it is
     * already frozen, nothing will be done - so, it's safe to call this method multiple times.
     * Because the atom instance is returned, this method may be called instead of <code>get</code>
     * to retrieve an atom instance based on its handle while making sure that it's never removed
     * from the cache. 
     * </p>
     * 
     * <p>
     * This method should be called with care since it is a possible source of memory leaks. The
     * <code>un-freeze</code> method should be called at appropriate times when you no longer need
     * an object to absolutely remain in main memory. Typically, freezing an atom is desirable in
     * the following situations:
     * 
     * <ul>
     * <li>You need to retrieve a <code>HGHandle</code> from a Java instance reference by a call
     * to the <code>getHandle</code>. This is only guaranteed to work when the atom is in the cache.</li>
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
    public Object freeze(final HGHandle handle)
    {
    	return this.getTransactionManager().ensureTransaction(new Callable<Object>() {
    		public Object call() {
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
    	});
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
    	return cache.getIncidenceCache().isLoaded(getPersistentHandle(h));
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
     * introspection mechanism.
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
            HGHandle type = typeSystem.getTypeHandle(value);
            if (type == null)
            	throw new HGException("Unable to create HyperGraph type for class " + value.getClass().getName());
            if (eventManager.dispatch(this, 
                    new HGAtomProposeEvent(atom, type, flags)) == HGListener.Result.cancel)
                 throw new HGAtomRefusedException();            
            result = addLink(value, type, link, (byte)flags);
        }
        else
        {
        	HGHandle type = typeSystem.getTypeHandle(atom);
        	if (type == null)
        		throw new HGException("Unable to create HyperGraph type for class " + atom.getClass().getName());
            if (eventManager.dispatch(this, 
                    new HGAtomProposeEvent(atom, type, flags)) == HGListener.Result.cancel)
                 throw new HGAtomRefusedException();        	
        	result = addNode(atom, type, (byte)flags);
        }
        eventManager.dispatch(this, new HGAtomAddedEvent(result, "HyperGraph.add"));
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
     * @throws HGAtomRefusedException if the {@link HGAtomProposeEvent} was rejected by a listener.
     */
    public HGHandle add(Object atom, HGHandle type, int flags)
    {
        if (eventManager.dispatch(this, 
               new HGAtomProposeEvent(atom, type, flags)) == HGListener.Result.cancel)
            throw new HGAtomRefusedException();
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
        if (atom instanceof HGGraphHolder)
            ((HGGraphHolder)atom).setHyperGraph(this);
        if (atom instanceof HGHandleHolder)
        	((HGHandleHolder)atom).setAtomHandle(result);
        eventManager.dispatch(this, new HGAtomAddedEvent(result, "HyperGraph.add"));
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
    			HGLiveHandle updated = cache.get(live.getPersistent());
    			if (updated != null)
    				return updated;
    			else
    				return live.getPersistent();
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
    public <T> T get(final HGHandle handle)
    {
//return getTransactionManager().ensureTransaction(new Callable<T>() 
//    	 	    { public T call() {
    	
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
        		HGLiveHandle existing = cache.get(liveHandle.getPersistent());
        		if (existing != null)
        		{
        			theAtom = (T)existing.getRef();
        			if (theAtom != null)
        			{
        				eventManager.dispatch(HyperGraph.this, new HGAtomAccessedEvent(existing, theAtom));
        				return (T)theAtom;
        			}
        		}
       			persistentHandle = liveHandle.getPersistent();
        	}
        	else
        	{
        		eventManager.dispatch(HyperGraph.this, new HGAtomAccessedEvent(liveHandle, theAtom));
        		return (T)theAtom;
        	}
        }
        else
            persistentHandle = (HGPersistentHandle)handle;
        
        Pair<HGLiveHandle, Object> loaded = loadAtom(persistentHandle, liveHandle);                

        if (loaded == null)
        	return null; 
        
        liveHandle = loaded.getFirst();
               
        eventManager.dispatch(HyperGraph.this, new HGAtomAccessedEvent(liveHandle, loaded.getSecond()));        
        return (T)loaded.getSecond();        
    }

    /**
     * <p>Return the handle of the specified atom.</p>
     * 
     * @param atom The atom whose handle is desired.
     * @return The <code>HGHandle</code> of the passed in atom, or <code>null</code>
     * if the atom is not in HyperGraph cache at the moment.
     */
    public HGHandle getHandle(final Object atom)
    {
        return cache.get(atom);
    }

    /**
     * <p>Retrieve the handle of the type of the atom referred to by <code>handle</code>.</p>
     * 
     * <p><strong>FIXME:</strong> Instances of the same run-time Java type are not guaranteed
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
    	Object atom = null;
    	if (handle instanceof HGLiveHandle)
    	{
    		atom = ((HGLiveHandle)handle).getRef();
    		pHandle = ((HGLiveHandle)handle).getPersistent();
    	}
    	else
    	{
    		pHandle = (HGPersistentHandle)handle;
    		HGLiveHandle lHandle = cache.get(pHandle);
    		if (lHandle != null)
    			atom = lHandle.getRef();
    	}
    	if (atom != null && atom instanceof HGTypeHolder)
    		return getHandle(((HGTypeHolder)atom).getAtomType());
    	HGPersistentHandle [] link = store.getLink(pHandle);
    	if (link == null || link.length < 2)
    		return null;
    	else
    		return refreshHandle(link[0]);
    }

    /**
     * <p>Remove an atom from the HyperGraph database. This is equivalent to calling
     * <code>remove(handle, false)</code> - see that version of <code>remove</code> for
     * detailed explanation. Essentially, this means that all links pointing to the
     * atom will be removed as well. This default behavior is based on the assumption
     * that most frequently links as ordered tuples that establish a particular 
     * relationship b/w their targets and therefore make sense only as a whole.</p>
     * 
     * @param handle The handle of the atom to be removed. <strong>NOTE:</strong> if no atom
     * exists with this handle (e.g. the atom was already removed), the method does nothing and
     * return false. If an attempt is made to remove an atom with a <code>null</code>
     * handle, then a regular <code>NullPointerException</code> is thrown.
     * @return <code>true</code> if the atom was successfully removed and <code>false</code>
     * otherwise.
     * @throws HGRemoveRefusedException if integrity constrains are violated or a user registered
     * listener to the {@link HGAtomRemoveRequestEvent} returns a {@link HGListener.Result.cancel}
     * result.
     */
    public boolean remove(final HGHandle handle)
    {
    	return remove(handle, config.isKeepIncidentLinksOnRemoval());
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
     * return false. If an attempt is made to remove an atom with a <code>null</code>
     * handle, then a regular <code>NullPointerException</code> is thrown.
     * @param keepIncidentLinks A flag indicating whether to remove the atom from the links
     * pointing to it (if <code>true</code>) or whether to remove the links altogether (if
     * <code>false</code>). The flag applies recursively to all removals triggered from
     * this call.
     * @return <code>true</code> if the atom was successfully removed and <code>false</code>
     * otherwise.
     * @throws HGRemoveRefusedException if integrity constrains are violated or a user registered
     * listener to the {@link HGAtomRemoveRequestEvent} returns a {@link HGListener.Result.cancel}
     * result.
     */
    public boolean remove(final HGHandle handle, final boolean keepIncidentLinks)
    {
    	if (eventManager.dispatch(this, new HGAtomRemoveRequestEvent(handle)) == HGListener.Result.cancel)
    		throw new HGRemoveRefusedException(handle, "Removal cancelled by atom listener");
    	    	
    	return getTransactionManager().ensureTransaction(new Callable<Boolean>() 
    	{ 
    		public Boolean call() 
    		{
    	    	if (config.getPreventDanglingAtomReferences())
    	    	{
    	    		AtomRefType refType = typeSystem.getAtomType(HGAtomRef.class);
    	    		 // symbolic links don't prevent removal of atoms
    				if (refType.getHardIdx().findFirst(handle.getPersistent()) != null ||
    					refType.getFloatingIdx().findFirst(handle.getPersistent()) != null)
    					throw new HGRemoveRefusedException(handle, "Atom is in use in a HGAtomRef");
    	    	}
    			
    			return removeTransaction(handle, keepIncidentLinks); 
    		}
    	});
    }
    
    private boolean removeTransaction(final HGHandle handle, final boolean keepIncidentLinks)
    {
        HGPersistentHandle pHandle = getPersistentHandle(handle);
        Set<HGPersistentHandle> inRemoval = TxAttribute.getSet(getTransactionManager(), 
        													   TxAttribute.IN_REMOVAL, 
        													   HashSet.class); 
        if (inRemoval.contains(handle))
        	return true;
        else
        	inRemoval.add(pHandle);
        
        try
        {
	        HGPersistentHandle [] layout = store.getLink(pHandle);        
	        
	        if (layout == null)
	            return false;
	        else if (layout[0].equals(typeSystem.getTop()))
	        	throw new HGRemoveRefusedException(handle, 
	        			"Cannot remove the HyperGraph primitive type: " + pHandle);
	        
	        Object atom = get(handle); // need the atom in order to clear all indexes...
	        
	        //
	        // If the atom is a type, remove it from the type system 
	        // (which also removes all its instances).
	        //
	        if (atom instanceof HGAtomType)
	        {	        		
	        	HGSearchResult<HGPersistentHandle> instances = null;
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
	        
	        //
	        // Clean all indexing entries related to this atom.
	        //
	        idx_manager.maybeUnindex(typeHandle, type, pHandle, atom);        
	        indexByType.removeEntry(typeHandle, pHandle);
	        indexByValue.removeEntry(valueHandle, pHandle);
	
	        //
	        // Remove the atom record from the store and cache.
	        //
	        TypeUtils.releaseValue(HyperGraph.this, type, valueHandle);
	        //type.release(valueHandle);         
	        store.removeLink(pHandle);
	        
	        //
	        // If it's a link, remove it from the incidence sets of all its 
	        // targets.
	        //
	        if (layout.length > 2)
	            for (int i = 2; i < layout.length; i++)
            		removeFromIncidenceSet(layout[i], pHandle);
	
	        //
	        // Handle links pointing to this atom:
	        //
	        if (keepIncidentLinks)        	
	        {
	        	IncidenceSet incidenceSet = cache.getIncidenceCache().get(pHandle); 
	            HGSearchResult<HGHandle> rsInc = incidenceSet.getSearchResult();
	            try { while (rsInc.hasNext()) targetRemoved(rsInc.next(), pHandle); }
	            finally { rsInc.close(); }        	
	        }
	        else
	        {
	        	IncidenceSet incidenceSet = cache.getIncidenceCache().getIfLoaded(pHandle);
	        	if (incidenceSet != null)
	        	{
		        	HGSearchResult<HGHandle> rsInc = incidenceSet.getSearchResult();
		        	try { while (rsInc.hasNext()) removeTransaction(rsInc.next(), false); }
		            finally { rsInc.close(); }		        	
	        	}
	        	else
	        	{
	        	    HGSearchResult<HGHandle> rsInc = (HGSearchResult)store.getIncidenceResultSet(pHandle);
	        	    try
	        	    {
	                    while (rsInc.hasNext())
	                        removeTransaction(rsInc.next(), false);	        	        
	        	    }
	        	    finally
	        	    {
	        	        rsInc.close();
	        	    }
	        	}   
//	        		for (HGPersistentHandle h : store.getIncidenceSet(pHandle))
//	        			removeTransaction(h, false);
	        }
	        store.removeIncidenceSet(pHandle);   
	        cache.getIncidenceCache().remove(pHandle);
	        cache.remove((HGHandle)cache.get(atom));        
	        eventManager.dispatch(HyperGraph.this, new HGAtomRemovedEvent(pHandle));
	        return true;
        }
        finally
        {
        	inRemoval.remove(pHandle);
        }
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
    public boolean  update(Object atom)
    {
    	HGHandle h = getHandle(atom);
    	if (h == null)
    		throw new HGException("Could not find HyperGraph handle for atom " + atom);
    	else
    		return replace(h, atom, getType(h));
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
     * type of the atom will be inferred using the Java introspection mechanism.
     */
    public boolean replace(HGHandle handle, Object atom)
    {
    	if (handle.equals(getHandle(atom)))
    		return replace(handle, atom, getType(handle)); // this is an 'update'
    	HGHandle atomType;
    	if (atom instanceof HGValueLink)    		
    	{
    		Class<?> c = ((HGValueLink)atom).getValue().getClass();
    		atomType = typeSystem.getTypeHandle(c);
            if (atomType == null)
            	throw new HGException("Unable to create HyperGraph type for class " + c.getName());
            
    	}
    	else
    	{
    		atomType = typeSystem.getTypeHandle(atom);
            if (atomType == null)
            	atomType = typeSystem.getTypeHandle(atom.getClass());
            if (atomType == null)            	
            	throw new HGException("Unable to create HyperGraph type for class " + atom.getClass().getName());    		
    	}
        return replace(handle, atom, atomType);        
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
    public boolean replace(HGHandle handle, Object atom, HGHandle type)
    {
        if (eventManager.dispatch(this, 
                   new HGAtomReplaceRequestEvent(handle, type, atom)) == HGListener.Result.cancel)
            return false;
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
            pHandle = lHandle.getPersistent();
        }
        
        replaceInternal(lHandle, pHandle, atom, type);
        eventManager.dispatch(this, new HGAtomReplacedEvent(lHandle));   
        return true;
    }
    
    /**
     * <p>
     * Put an existing atom into this HyperGraph instance. This is a rather low-level method
     * that requires you to explicitly find the type and value handles for the atom and use
     * an already existing, yet unknown to this HyperGraph instance, persistent handle. 
     * </p>
     * 
     * <p>
     * One possible use of this is when an application relies on a HyperGraph for storage and it needs
     * to populate it with some predefined set of atoms with a set of existing, pre-fabricated handles.
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
     * @param instance The runtime instance of the atom. The runtime instance is needed in order to
     * update any indices related to the atom. If this parameter is <code>null</code>, an attempt to 
     * construct the instance from the <code>valueHandle</code> will be made.
     */
    public void define(final HGHandle atomHandle, 
    				   final HGHandle typeHandle, 
    				   final HGHandle valueHandle, 
    				   final HGLink   outgoingSet,
    				   final Object instance,
    				   final int flags)
    {
        if (eventManager.dispatch(this, 
                new HGDefineProposeEvent(atomHandle, instance, typeHandle, flags)) == HGListener.Result.cancel)
             throw new HGAtomRefusedException();
    	getTransactionManager().ensureTransaction(new Callable<Object>() 
    	{ public Object call() {
    	    if (get(atomHandle) != null)
    	        throw new IllegalArgumentException("Can't define an already existing atom " + atomHandle + ", please use replace instead.");
	    	HGPersistentHandle [] layout = new HGPersistentHandle[outgoingSet == null ? 2 : 2 + outgoingSet.getArity()];
	    	layout[0] = getPersistentHandle(typeHandle);
	    	layout[1] = getPersistentHandle(valueHandle);
	    	if (outgoingSet != null)
	    		for (int i = 0; i < outgoingSet.getArity(); i++)
	    			layout[i + 2] = getPersistentHandle(outgoingSet.getTargetAt(i));
	    	store.store(atomHandle.getPersistent(), layout);
	    	indexByType.addEntry(layout[0], atomHandle.getPersistent());
	    	indexByValue.addEntry(layout[1], atomHandle.getPersistent());
            HyperGraph.this.atomAdded(atomHandle.getPersistent(), instance, flags);	    		    	
	    	ReadyRef<HGHandle[]> linkRef = null;
	    	if (outgoingSet != null)
	    	{
	    		HGHandle [] targets = new HGHandle[outgoingSet.getArity()];
		    	System.arraycopy(layout, 2, targets, 0, targets.length);
		    	updateTargetsIncidenceSets(atomHandle.getPersistent(), outgoingSet);
		    	linkRef = new ReadyRef<HGHandle[]>(targets);
    		}
            HGAtomType type = get(typeHandle);	    	
	        idx_manager.maybeIndex(layout[0], 
	        					   type, 
	        					   atomHandle.getPersistent(),
	        					   instance == null ? type.make(layout[1], linkRef, null) : instance);	        
	    	return null;
    	}});
        eventManager.dispatch(this, new HGAtomDefinedEvent(atomHandle, "HyperGraph.define"));    	
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
     */
    public void define(final HGHandle atomHandle, final Object instance, final int flags)
    {
    	HGHandle typeHandle = null;
    	if (instance == null)
    		typeHandle = typeSystem.getNullType();
    	else if (instance instanceof HGValueLink)
    		typeHandle = typeSystem.getTypeHandle(((HGValueLink)instance).getValue().getClass());
    	else
    	    typeHandle = typeSystem.getTypeHandle(instance.getClass());
    	if (typeHandle == null)
    		throw new HGException("Could not find HyperGraph type for object of type " + instance.getClass());
    	define(atomHandle, typeHandle, instance, flags);
    }

    /**
     * <p>
     * A version of <code>define</code> allowing one to pass a specific type to use when
     * storing the atom.
     * </p>
     * 
     * @param atomHandle The atom handle.
     * @param typeHandle The handle of the type to use.
     * @param instance The atom instance.
     * @param flags System flags.
     */
    public void define(final HGHandle atomHandle, 
                       final HGHandle typeHandle, 
                       final Object instance, 
                       final int flags)
    {
        getTransactionManager().ensureTransaction(new Callable<Object>() 
          { public Object call() {        
              HGAtomType type = typeSystem.getType(typeHandle);
              HGLink link = null;
              Object payload = instance;
              if (instance instanceof HGLink)
              {
                  link = (HGLink)instance;
                  if (instance instanceof HGValueLink)
                      payload = ((HGValueLink)instance).getValue();
              }
              HGPersistentHandle valueHandle = TypeUtils.storeValue(HyperGraph.this, payload, type);
              if (instance instanceof HGTypeHolder)
                  ((HGTypeHolder<HGAtomType>)instance).setAtomType(type);
              define(atomHandle, typeHandle, valueHandle, link, instance, flags);
              return null;
          }});        
    }
    
    /**
     * <p>Delegate to <code>define(HGPersistentHandle, Object, HGHandle [], byte)</code> with the
     * flags parameter = 0.
     *  
     * @param atomHandle The handle of the atom to define.
     * @param instance The atom's runtime instance.
     */
    public void define(final HGHandle atomHandle, final Object instance)
    {
    	define(atomHandle, instance, (byte)0);
    }
    
    /**
     * <p>Return the <code>IncidenceSet</code>, that is the set of all <code>HGLink</code>s pointing 
     * to, the atom referred by the passed in handle.</p>
     * 
     * @param handle The handle of the atom whose incidence set is desired.
     * @return The atom's <code>IncidenceSet</code>. 
     * The returned set may have 0 elements, but it will never be <code>null</code>.
     */
    public IncidenceSet getIncidenceSet(HGHandle handle)
    {
    	return cache.getIncidenceCache().get(getPersistentHandle(handle));
    }
    
    public int getSystemFlags(HGHandle handle)
    {
        if (!config.isUseSystemAtomAttributes())
            return 0;
        else if (handle instanceof HGLiveHandle)
    		if (handle instanceof HGManagedLiveHandle)
    			return ((HGManagedLiveHandle)handle).getFlags();
    		else
    			return 0;
    	else
    	{
    		HGAtomAttrib attribs = this.getAtomAttributes((HGPersistentHandle)handle);
    		if (attribs != null)
    			return attribs.flags;
    		else
    			return 0;
    	}
    }
    
    public void setSystemFlags(final HGHandle handle, final int flags)
    {
        if (!config.isUseSystemAtomAttributes())
            return;
    	getTransactionManager().ensureTransaction(new Callable<Object>() 
    	{ public Object call() {
	    	//
	    	// NOTE: there are several cases here. We may be switching from
	    	// default to non-default or vice-versa. We may be switching from
	    	// managed to non-managed or vice-versa. In the first situation, we
	    	// need to take care of adding/removing atom attributes. In all cases,
	    	// we have to adjust the live handle from/to managed/normal.
	    	//
	    	HGPersistentHandle pHandle = getPersistentHandle(handle);
	    	HGAtomAttrib attribs = HyperGraph.this.getAtomAttributes(pHandle);
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
	    		attribs = new HGAtomAttrib();
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
	    			cache.atomRead(pHandle, instance, attribs);
	    	}
	    	return null;
    	}});
    }
    
    public <T> T findOne(HGQueryCondition condition)
    {
        return (T)hg.findOne(this, condition);
    }
    
    public <T> T getOne(HGQueryCondition condition)
    {
        return (T)hg.getOne(this, condition);
    }
    
    /**
     * <p>Run a HyperGraphDB lookup query based on the specified condition.</p>
     * 
     * @param condition The <code>HGQueryCondition</code> constraining the returned
     * result set. It cannot be <code>null</code>.
     */
    public <T> HGSearchResult<T> find(HGQueryCondition condition)
    {
    	HGQuery<T> query = HGQuery.make(this, condition);
        return query.execute();
    }
    
    public <T> List<T> getAll(HGQueryCondition condition)
    {
        return hg.getAll(this, condition);
    }
    
    public List<HGHandle> findAll(HGQueryCondition condition)
    {
        return hg.findAll(this, condition);
    }
    
    public long count(HGQueryCondition condition)
    {
        return hg.count(this, condition);
    }
    
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
    	return getTransactionManager().ensureTransaction(new Callable<HGLiveHandle>() 
    	{ public HGLiveHandle call() {
	    	HGAtomType type = typeSystem.getType(typeHandle);
	    	HGPersistentHandle pTypeHandle = getPersistentHandle(typeHandle);    	
	        HGPersistentHandle valueHandle = TypeUtils.storeValue(HyperGraph.this, payload, type);  
	
	        HGPersistentHandle [] layout = new HGPersistentHandle[2];            
	        layout[0] = pTypeHandle;
	        layout[1] = valueHandle;
	        final HGLiveHandle lHandle = atomAdded(store.store(layout), payload, flags);
	        if (payload instanceof HGTypeHolder)
	        	((HGTypeHolder<HGAtomType>)payload).setAtomType(type);    	        	        	        
	        indexByType.addEntry(pTypeHandle, lHandle.getPersistent());
	        indexByValue.addEntry(valueHandle, lHandle.getPersistent());
	        idx_manager.maybeIndex(pTypeHandle, type, lHandle.getPersistent(), payload);	        
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
    	return getTransactionManager().ensureTransaction(new Callable<HGLiveHandle>() 
        { public HGLiveHandle call() {
	    	HGAtomType type = typeSystem.getType(typeHandle);
	    	HGPersistentHandle pTypeHandle = getPersistentHandle(typeHandle);
	        HGPersistentHandle valueHandle = TypeUtils.storeValue(HyperGraph.this, payload, type);            
	        
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
	        HGPersistentHandle pHandle = store.store(layout);	        
	        HGLiveHandle lHandle = atomAdded(pHandle, outgoingSet, flags);
	        if (payload instanceof HGTypeHolder)
	        	((HGTypeHolder<HGAtomType>)payload).setAtomType(type);    	        	        	        
	        indexByType.addEntry(pTypeHandle, pHandle);
	        indexByValue.addEntry(valueHandle, pHandle);
	        idx_manager.maybeIndex(pTypeHandle, type, pHandle, payload);	
	        //
	        // Update the incidence sets of all its targets.
	        //
	        updateTargetsIncidenceSets(pHandle, outgoingSet);	        
	        return lHandle;
    	}});
    }
    
    private HGLiveHandle atomAdded(HGPersistentHandle pHandle, Object instance, int flags)
    {
    	if (instance instanceof HGGraphHolder)
    		((HGGraphHolder)instance).setHyperGraph(HyperGraph.this);
    	
    	HGLiveHandle lHandle;
			if (config.isUseSystemAtomAttributes())
      {
       	HGAtomAttrib attribs = new HGAtomAttrib();
       	attribs.flags = (byte)flags;
       	attribs.retrievalCount = 1;
       	attribs.lastAccessTime = System.currentTimeMillis();
       	setAtomAttributes(pHandle, attribs);        	
       	lHandle = cache.atomAdded(pHandle, instance, attribs);
      }        
			else
			{
				HGAtomAttrib attribs = new HGAtomAttrib();            
				if (config.isUseSystemAtomAttributes() && flags != 0)
				{
					attribs.flags = (byte)flags;
					setAtomAttributes(pHandle, attribs);
				}
				lHandle = cache.atomAdded(pHandle, instance, attribs);
			}
			if (instance instanceof HGHandleHolder)
				((HGHandleHolder)instance).setAtomHandle(lHandle);
			return lHandle;
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
    	return getTransactionManager().ensureTransaction(new Callable<Pair<HGLiveHandle, Object>>() 
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
	            
	        if (typeHandle.equals(typeSystem.getTop()))
	        {
	        	HGLiveHandle result = typeSystem.loadPredefinedType(persistentHandle); 
	        	return new Pair<HGLiveHandle, Object>(result, result.getRef());
	        }
	        
	        IncidenceSetRef isref = new IncidenceSetRef(persistentHandle, HyperGraph.this);
	        
	        HGAtomType type = typeSystem.getType(typeHandle);
	        boolean topCall = TypeUtils.initThreadLocals();
	        try
	        {
		        if (type == null)
		            throw new HGException("Unable to find type with handle " + typeHandle + " in database.");
		        if (link.length == 2)	        	
		            instance = type.make(valueHandle, EMPTY_HANDLE_SET_REF, isref);
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
	        }
	        finally
	        {
	        	TypeUtils.releaseThreadLocals(topCall);
	        }
            if (instance instanceof HGAtomType)
                instance = typeSystem.toRuntimeInstance(persistentHandle, (HGAtomType)instance);
	        
	        HGLiveHandle result = null;
	        if (liveHandle == null)
	        {
	        	HGAtomAttrib attribs = config.isUseSystemAtomAttributes() ? getAtomAttributes(persistentHandle) : new HGAtomAttrib();
       			result = cache.atomRead(persistentHandle, instance, attribs);
       			// The method could return an existing live handle, already in the cache
       			// we detect this by finding that the reference that handle holds is != from 
       			// the instance we just loaded from disk
       			Object existing = result.getRef();
       			if (existing != instance)
       			{       				
       				if (existing != null)
       				{
       					instance = existing;
       				}
       				else
       				{
       					result = cache.atomRefresh(result, instance, false);
       				}
       			}
	        }
	        else
	        {
	        	result = cache.atomRefresh(liveHandle, instance, false);
	        }
	        
	        if (instance instanceof HGGraphHolder)
	        	((HGGraphHolder)instance).setHyperGraph(HyperGraph.this);
	        if (instance instanceof HGHandleHolder)
	        	((HGHandleHolder)instance).setAtomHandle(result);
	        if (instance instanceof HGTypeHolder)
	        	((HGTypeHolder<HGAtomType>)instance).setAtomType(type);
	    	
	        eventManager.dispatch(HyperGraph.this, new HGAtomLoadedEvent(result, instance));
	        
	        //
	        // If the incidence set of the newly fetched atom is already loaded,
	        // traverse it to update the target handles of all links pointing to it.
	        //
	        IncidenceSet incidenceSet = cache.getIncidenceCache().getIfLoaded(persistentHandle);
	        if (incidenceSet != null)
	        	updateLinksInIncidenceSet(incidenceSet, result);
	        
	        //
	        // If the newly fetched atom is a link, update all loaded incidence
	        // sets, of which it is part, with its live handle. 
	        //
	        // NOTE: commented out for now since IncidenceSet stores only persistent handles for
	        // speedier lookup (because this way there's no need to check for live handles and do type casts)
	        /* if (liveHandle.getRef() instanceof HGLink)
	        {
	        	HGLink link = (HGLink)liveHandle.getRef();
	            for (int i = 0; i < link.getArity(); i++)
	            {
	                IncidenceSet targetIncidenceSet = cache.getIncidenceSet(getPersistentHandle(link.getTargetAt(i)));
	                if (targetIncidenceSet != null)
	                    for (int j = 0; j < targetIncidenceSet.length; j++)
	                    {
	                        if (targetIncidenceSet[j].equals(persistentHandle))
	                        	targetIncidenceSet[j] = liveHandle;
	                    }
	            }
	        } */
	        
	        return new Pair<HGLiveHandle, Object>(result, instance);
    	}}, HGTransactionConfig.READONLY);
    }
    
//    private void unloadAtom(final HGLiveHandle lHandle, final Object instance)
//    {
//    	try
//    	{
//	    	getTransactionManager().ensureTransaction(new Callable<Object>() 
//	  	    { public Object call() {
//	  	    	
//	  	    	// The following (both the code for MUTABLE and MANAGED flags) cause
//	  	    	// deadlocks with a 'get' operation in the PhnatomHandle while
//	  	    	// an atom is waiting to be de-queued: graph.get(handle) blocks 
//	  	    	// on waiting for the PhantomHandle to return, which in turn waits
//	  	    	// for the atom (currently being GC-ed) to be dequeued, but this 
//	  	    	// doesn't happen because unloadAtom can't proceed due to simultaneous
//	  	    	// DB write with the get operation.
///*		    	if ((lHandle.getFlags() & HGSystemFlags.MUTABLE) != 0)
//		    	{
//		    		//TODO: Maybe this should be done somewhere else or differently...
//		    		//in atomAdded() attribs are added only for MANAGED flag
//		    		AtomAttrib attrib = getAtomAttributes(lHandle.getPersistentHandle());
//		    		if(attrib == null){
//		    			attrib = new AtomAttrib();
//		    			attrib.flags = lHandle.getFlags();
//		    		   setAtomAttributes(lHandle.getPersistentHandle(), attrib);
//		    		}
//		    		//
//		    		// We don't explicitly track what has changed in atom. So
//		    		// we need to save its "whole" value. Because, the replace
//		    		// operation is too general and it may interact with the cache
//		    		// in complex ways, while this method may be called during cache
//		    		// cleanup, we can't use 'replace'. We need a separate version
//		    		// that is careful not to use the cache.
//		    		//
//		    		 // rawSave(lHandle.getPersistentHandle(), instance);
//		    		replace(lHandle, instance);
//		    	} 
//		    	if ((lHandle.getFlags() & HGSystemFlags.MANAGED) != 0)
//		    	{
//		    		HGManagedLiveHandle mHandle = (HGManagedLiveHandle)lHandle;
//		    		AtomAttrib attrib = getAtomAttributes(mHandle.getPersistentHandle());
//		    		attrib.flags = mHandle.getFlags();
//		    		attrib.retrievalCount += mHandle.getRetrievalCount();
//		    		attrib.lastAccessTime = Math.max(mHandle.getLastAccessTime(), attrib.lastAccessTime);
//		    		setAtomAttributes(lHandle.getPersistentHandle(), attrib);
//		    	} */
//		    	return null;
//	    	}});
//    	}
//    	catch (HGException ex)
//    	{
//    		throw new HGException("Problem while unloading atom " + 
//					  			  instance + " of type " + instance.getClass().getName() + " " + ex.getMessage(),
//					  			  ex);
//    	}
//    	catch (Throwable t)
//    	{
//    		throw new HGException("Problem while unloading atom " + 
//		  			  instance + " of type " + instance.getClass().getName(),
//		  			  t);
//    	}    	
//    }
    
    void updateLinksInIncidenceSet(IncidenceSet incidenceSet, HGLiveHandle liveHandle)
    {
    	HGSearchResult<HGHandle> rs = incidenceSet.getSearchResult();
    	try
    	{
	    	while (rs.hasNext())
	        {
	        	HGLiveHandle lh = cache.get((HGPersistentHandle)rs.next());
	        	if (lh != null)
	            {
	                HGLink incidenceLink = (HGLink)lh.getRef();
	                if (incidenceLink != null) // ref may be null because of cache eviction
	                	updateLinkLiveHandle(incidenceLink, liveHandle);
	            }
	        }
    	}
    	finally
    	{
    		rs.close();
    	}
    }
    
    /**
     * Update a link to point to a "live" target instead of holding a 
     * persistent handle. This is slightly inefficient as it needs
     * to loop through all targets of the link. It is here perhaps that
     * a distinction b/w ordered and unordered links might become useful
     * for efficiency purposes: an unordered link would be able to use
     * a hash lookup on its handles to find the one that needs to be updated,
     * instead of a linear traversal. On the other, link arities tend to be
     * rather small, usually 2-3, so it shouldn't be a problem. 
     */
    void updateLinkLiveHandle(HGLink link, HGLiveHandle lHandle)
    {
        int arity = link.getArity();
        for (int i = 0; i < arity; i++)
        {
            HGHandle current = link.getTargetAt(i);
            if (current == lHandle)
                return;
            else if (current.equals(lHandle.getPersistent()))
            {
                link.notifyTargetHandleUpdate(i, lHandle);
                return;
            }
        }
    }
        
    void updateTargetIncidenceSet(HGPersistentHandle targetHandle, HGPersistentHandle linkHandle)
    {
        store.addIncidenceLink(targetHandle, linkHandle);            
        IncidenceSet targetIncidenceSet = cache.getIncidenceCache().getIfLoaded(targetHandle);
        if (targetIncidenceSet != null)
        	targetIncidenceSet.add(linkHandle);    	
    }
    
    void updateTargetsIncidenceSets(HGPersistentHandle atomHandle, HGLink link)
    {
        for (int i = 0; i < link.getArity(); i++)
        	updateTargetIncidenceSet(getPersistentHandle(link.getTargetAt(i)), atomHandle);                   
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
    		replaceTransaction(cache.get(l), getPersistentHandle(linkHandle), l, getType(linkHandle));
    	}
    }
    
    /**
     * Remove a link from the incidence set of a given atom. If the link is
     * not part of the incidence set of the <code>targetAtom</code>, nothing
     * is done.
     * 
     * @param targetAtom The handle of the atom whose incidence set need modification.
     * @param incidentLink The <code>HGPersistentHandle</code> of the link to be
     * removed - cannot be <code>null</code>.
     */
    private void removeFromIncidenceSet(HGPersistentHandle targetAtom,
    									HGPersistentHandle incidentLink)
    {       
//        Set<HGPersistentHandle> inRemoval = TxAttribute.getSet(getTransactionManager(), 
//															   TxAttribute.IN_REMOVAL, 
//															   HashSet.class);
        // Can't remember why atoms currently being removed should keep there incidence sets
        // intact, next time a bug shows up related to this, please comment on why the following
        // check is need.
        // -Borislav
        //
        // However, if the following is uncommented, there's a problem when deleting
        // types: the HGSubsumes links get removed during the type removal transaction
        // but they remain in the incidence set (due to the following line) which subsequently
        // causes an NPE.
        //
//        if (inRemoval.contains(targetAtom))
//        	return;
    	store.removeIncidenceLink(targetAtom, incidentLink);
        IncidenceSet targetIncidenceSet = cache.getIncidenceCache().getIfLoaded(targetAtom);
        if (targetIncidenceSet != null)
        	targetIncidenceSet.remove(incidentLink);
    }

    /**
     * Save the run-time value of an atom back to the database store, without
     * disrupting the cache. It is assumed 
     * 
     * @param handle The persistent handle of the atom.
     * @param instance The run-time value of the atom.
     */
/*    private void rawSave(HGPersistentHandle handle, Object instance)
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
    } */
    
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
//    	TypeUtils.initiateAtomConstruction(HyperGraph.this, layout[1]);
    	Object result = type.make(layout[1], 
    							  new ReadyRef<HGHandle[]>(targetSet), 
    							  new IncidenceSetRef(atomHandle, this));
//    	TypeUtils.atomConstructionComplete(HyperGraph.this, layout[1]);
        if (targetSet.length > 0 && ! (result instanceof HGLink))
            result = new HGValueLink(result, targetSet);
        if (result instanceof HGAtomType)
        	result = typeSystem.toRuntimeInstance(atomHandle, (HGAtomType)result);
        if (result instanceof HGGraphHolder)
            ((HGGraphHolder)result).setHyperGraph(this);
        return result;    	
    }

    /**
     * Replace an atom with a new value within a newly created transaction.
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
    	Object oldValue = getTransactionManager().ensureTransaction(new Callable<Object>() 
    	{ 
    		public Object call() { return replaceTransaction(lHandle, pHandle, atom, typeHandle); }
    	});    	
    	if (oldValue instanceof HGHandleHolder)
    		((HGHandleHolder)oldValue).setAtomHandle(null);
    }
    
    
    /**
     * Replace an atom with a new value. Recursively replace the values of type atoms.
     * 
     * @param lHandle The live handle of the atom, if available, can be null...
     * @param pHandle The persistent handle of the atom, can't be null
     * @param atom The new value of the atom
     * @param typeHandle The type of the new value
     */
    private Object replaceTransaction(HGLiveHandle lHandle, 
    							 	final HGPersistentHandle pHandle, 
    							 	final Object atom, 
    							 	final HGHandle typeHandle)
    {        
        Object newValue = atom;
        if (atom instanceof HGValueLink)
        	newValue = ((HGValueLink)atom).getValue();

        HGPersistentHandle [] layout = store.getLink(pHandle);
        HGPersistentHandle oldValueHandle = layout[1];        
        HGPersistentHandle oldTypeHandle = layout[0];
        HGAtomType oldType = (HGAtomType)get(oldTypeHandle);
        HGAtomType type = (HGAtomType)get(typeHandle);        

        Object oldValue = null;
        if (lHandle != null)
            oldValue = lHandle.getRef();
        if (oldValue == null || oldValue == atom)
            oldValue = rawMake(layout, oldType, pHandle); //rawMake will just construct the instance, without adding to cache
                
        idx_manager.maybeUnindex(oldTypeHandle.getPersistent(), oldType, pHandle, oldValue);
        
        if (oldValue instanceof HGValueLink)
        	oldValue = ((HGValueLink)oldValue).getValue();
	        
	    	//
	    	// If the atom is a type, we need to morph all its values to the new
	    	// type. This is done simply by recursively replacing instances
	    	// based on the old type atom with instances of the new type. 
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

	    	TypeUtils.releaseValue(HyperGraph.this, oldType, layout[1]);
	    	//oldType.release(layout[1]);
	    	if (newValue instanceof HGAtomType)
	    		layout[1] = TypeUtils.storeValue(
	    				this, 
	    				typeSystem.getSchema().fromRuntimeType(pHandle, (HGAtomType)newValue), 
	    				type);
	    	else
	    		layout[1] = TypeUtils.storeValue(this, newValue, type);
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
	    		HashSet<HGPersistentHandle> newTargets = new HashSet<HGPersistentHandle>();
	    		for (int i = 0; i < newLink.getArity(); i++)
	    		{
	    			HGPersistentHandle target = getPersistentHandle(newLink.getTargetAt(i)); 
	    			newLayout[2 + i] = target;
	    			newTargets.add(target);
	    		}    		
	    		for (int i = 2; i < layout.length; i++)
	    			if (!newTargets.remove(layout[i])) // remove targets that were there before, so we don't touch them below
	    				removeFromIncidenceSet(layout[i], pHandle);
	    		for (HGPersistentHandle newTarget : newTargets)
	    			updateTargetIncidenceSet(newTarget, pHandle);
	    	}
	    	else 
	    	{
	    		newLayout = new HGPersistentHandle[2];
	    		for (int i = 2; i < layout.length; i++)
	    			removeFromIncidenceSet(layout[i], pHandle);
	    	}
	    	
	    	newLayout[0] = layout[0];
	    	newLayout[1] = layout[1];    	
	    	store.store(pHandle, newLayout);
	    	
	    	idx_manager.maybeIndex(getPersistentHandle(typeHandle), type, pHandle, atom);
	    	if (atom instanceof HGGraphHolder)
	    		((HGGraphHolder)atom).setHyperGraph(this);
	    	if (lHandle != null)
		    	lHandle = cache.atomRefresh(lHandle, atom, true);
	        if (atom instanceof HGHandleHolder)
	        	 ((HGHandleHolder)atom).setAtomHandle(lHandle != null ? lHandle : pHandle);
	        return oldValue;
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
    	TypeUtils.releaseValue(this, oldType, layout[1]);
    	//2012.08.07 hilpold already called in previous method: oldType.release(layout[1]);
    	indexByValue.removeEntry(layout[1], instanceHandle);
    	layout[1] = TypeUtils.storeValue(this, oldInstance, newType);
    	indexByValue.addEntry(layout[1], instanceHandle);
    	store.store(instanceHandle, layout);
    	
    	Object newInstance = rawMake(layout, newType, instanceHandle);
		
    	HGLiveHandle instanceLiveHandle = cache.get(instanceHandle);
    	if (instanceLiveHandle != null && instanceLiveHandle.getRef() != null)
    		cache.atomRefresh(instanceLiveHandle, newInstance, true);
		
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
    
    private void initAtomManagement()
    {        
    	//
    	// Init HGStats atom.
    	//
        getTransactionManager().transact(new Callable<Object>() { public Object call() {
        statsHandle = hg.findOne(HyperGraph.this, hg.type(HGStats.class));
        if (statsHandle == null)
            statsHandle = add(stats);
        else
            stats = get(statsHandle);
        return null; }});
               
//    	HGSearchResult<HGPersistentHandle> rs = null;
//    	
//    	try
//    	{
//	    	rs = find(new AtomTypeCondition(typeSystem.getTypeHandle(HGStats.class)));
//	    	if (rs.hasNext())
//	    	{
//	    		statsHandle = rs.next();
//	    		stats = (HGStats)get(statsHandle);
//	    	}
//	    	else
//	    	{
//	    		statsHandle = add(stats);
//	    	}
//    	}
//    	catch (HGException ex)
//    	{
//    		throw ex;
//    	}
//    	catch (Throwable t)
//    	{
//    		throw new HGException(t);
//    	}
//    	finally
//    	{
//    		if (rs != null) rs.close();
//    	}
//    	
//    	eventManager.addListener(
//    			HGAtomEvictEvent.class,
//    			new HGListener()
//    			{
//    				public HGListener.Result handle(HyperGraph hg, HGEvent event)
//    				{
//    					HGAtomEvictEvent ev = (HGAtomEvictEvent)event;
//    					unloadAtom((HGLiveHandle)ev.getAtomHandle(), ev.getInstance());
//    					return Result.ok;
//    				}
//    			}
//    			);    	
    }
    
    private void loadListeners()
    {
    	HGSearchResult<HGHandle> rs = null;
    	try
    	{
    		rs = find(new AtomTypeCondition(typeSystem.getTypeHandle(HGListenerAtom.class)));
    		while (rs.hasNext())
    		{
    			HGListenerAtom listenerAtom = (HGListenerAtom)get((HGHandle)rs.next());
    			Class<?> eventClass;
    			Class<?> listenerClass;
    			try
    			{
    				eventClass = HGUtils.loadClass(this,listenerAtom.getEventClassName());
    				listenerClass = Class.forName(listenerAtom.getListenerClassName());
    				eventManager.addListener((Class<HGEvent>)eventClass, (HGListener)listenerClass.newInstance());
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
    
    private HGAtomAttrib getAtomAttributes(HGPersistentHandle handle)
    {
    	return systemAttributesDB.findFirst(handle);    	
    }
    
    private void setAtomAttributes(HGPersistentHandle handle, HGAtomAttrib attribs)
    {
    	systemAttributesDB.removeAllEntries(handle);
    	systemAttributesDB.addEntry(handle, attribs);
    }
    
    private void removeAtomAttributes(HGPersistentHandle handle)
    {
    	systemAttributesDB.removeAllEntries(handle);
    }
    
    protected void finalize() throws Throwable
    {
   		try { close(); } catch (Throwable t) { }
    }
}
