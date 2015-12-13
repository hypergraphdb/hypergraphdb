/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.cache.WeakRefAtomCache;
import org.hypergraphdb.event.HGDefaultEventManager;

import org.hypergraphdb.event.HGEventManager;
import org.hypergraphdb.query.HGQueryConfiguration;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.type.HGTypeConfiguration;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * A bean that holds configuration parameters for a HyperGraphDB initialization.
 * An instance can be passed to the {@link HGEnvironment#configure(String, HGConfiguration)} or 
 * {@link HGEnvironment#get(String, HGConfiguration)} methods.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGConfiguration
{	
	private HGHandleFactory handleFactory;
	private HGStoreImplementation storeImplementation;
	private boolean transactional;
	private boolean skipMaintenance;
	private boolean cancelMaintenance;
	private boolean skipOpenedEvent;
	private boolean preventDanglingAtomReferences = true; 
	private int maxCachedIncidenceSetSize; 
	private boolean useSystemAtomAttributes;
	private boolean keepIncidentLinksOnRemoval = false;
	private HGTypeConfiguration typeConfiguration = new HGTypeConfiguration();
	private HGQueryConfiguration queryConfiguration = new HGQueryConfiguration();
	private HGAtomCache cacheImplementation = new WeakRefAtomCache();
	private HGEventManager eventManager = new HGDefaultEventManager();
	private ClassLoader classLoader;
	
	public HGConfiguration()
	{
		resetDefaults();
	}
		
	/**
	 * <p>
	 * Return the handle factory configured for this database. If no factory has been
	 * explicitly configured, a default implementation based on UUIDs is provided.
	 * </p>
	 */
    public HGHandleFactory getHandleFactory()
    {
        if (handleFactory == null)
            handleFactory = HGUtils.getImplementationOf(HGHandleFactory.class.getName(), 
                                    "org.hypergraphdb.handle.UUIDHandleFactory");
        return handleFactory;
    }


    /**
     * <p>
     * Set the persistent handle factory for this database. 
     * </p>
     *  
     * @param handleFactory The handle factory instance.
     */
    public void setHandleFactory(HGHandleFactory handleFactory)
    {
        this.handleFactory = handleFactory;
    }

    /**
     *<p>
     * Return the low-level storage implementation to be used for this
     * database instance.
     * </p>
     */
    public HGStoreImplementation getStoreImplementation()
    {
        if (storeImplementation == null)
            storeImplementation = HGUtils.getImplementationOf(HGStoreImplementation.class.getName(), 
                    "org.hypergraphdb.storage.bje.BJEStorageImplementation");
        return storeImplementation;
    }

    /**
     * <p>
     * Specify the low-level storage implementation to be used for this database instance.
     * </p>
     * @param storeImplementation
     */
    public void setStoreImplementation(HGStoreImplementation storeImplementation)
    {
        this.storeImplementation = storeImplementation;
    }

    /**
	 * <p>Set all parameters of this configuration to their default values.</p>
	 */
	public void resetDefaults()
	{
		this.transactional = true;
		this.skipMaintenance = false;
		this.cancelMaintenance = false;
		this.skipOpenedEvent = false;
		this.maxCachedIncidenceSetSize = 10000;
		this.useSystemAtomAttributes = true;
	}
	
	/**
	 * <p>
	 * <code>true</code> if the database is configured to support transactions and <code>false</code>
	 * otherwise.
	 * </p>
	 * 
	 * @return
	 */
	public boolean isTransactional()
	{
		return transactional;
	}

	/**
	 * <p>
	 * Specifies if the database should be opened in transactional mode which is the default 
	 * mode. Setting this flag to false should be done with care. It results in much faster 
	 * operations (4-5 times faster), but it can result in an unrecoverable crash. In general
	 * this should be used when a lot of data is being loaded into a brand new, or a properly
	 * backed up beforehand, database.
	 * </p>
	 * 
	 * <p>
	 * Note that being <em>transactional</em> is not a property of the database instance, but
	 * rather of the current interaction session.  
	 * </p>
	 * 
	 * @param transactional
	 */
	public void setTransactional(boolean transactional)
	{
		this.transactional = transactional;
	}
	
    /** 
	 * <p>Return true if HyperGraph should skip scheduled maintenance operations when
	 * it is open. Return false otherwise.</p> 
	 */
	public boolean getSkipMaintenance()
	{
		return skipMaintenance;
	}

	/**
	 * <p>Specify whether HyperGraph should skip maintenance operation when it is being open.</p>
	 */
	public void setSkipMaintenance(boolean skipMaintenance)
	{
		this.skipMaintenance = skipMaintenance;
	}

	/**
	 * <p>Return true if HyperGraph will cancel maintenance operation when it is being open.
	 * Canceling maintenance operations will delete them so they'll never be run.</p>
	 */	
	public boolean getCancelMaintenance()
	{
		return cancelMaintenance;
	}

	/**
	 * <p>Specify whether HyperGraph should cancel maintenance operation when it is being open.
	 * Canceling maintenance operations will delete them so they'll never be run.</p>
	 */		
	public void setCancelMaintenance(boolean cancelMaintenance)
	{
		this.cancelMaintenance = cancelMaintenance;
	}

	/**
	 * <p>
	 * Return <code>true</code> if the startup process should <strong>NOT</strong> 
	 * fire a <code>HGOpenedEvent</code> so none of the registered listeners will
	 * be triggered.
	 * </p> 
	 */
    public boolean getSkipOpenedEvent()
    {
        return skipOpenedEvent;
    }

    /**
     * <p>
     * Specify whether the startup process should <strong>NOT</strong> 
     * fire a <code>HGOpenedEvent</code> so none of the registered listeners will
     * be triggered. This is useful whenever a HyperGraphDB instance should be
     * opened "merely" for examining/querying data and user-defined bootstrap
     * operations are to be skipped.
     * </p> 
     * 
     * <p>
     * Note that setting this flag to <code>true</code> is meaningful only
     * when you have defined one or more listeners to the <code>HGOpenedEvent</code>.
     * For example, a brand new HyperGraphDB instance will not have any such
     * listeners defined.
     * </p>
     */
    public void setSkipOpenedEvent(boolean skipOpenedEvent)
    {
        this.skipOpenedEvent = skipOpenedEvent;
    }

    /**
     * <p>Return the configured maximum size of atom incidence sets that are kept in 
     * RAM. Small incidence sets are kept in a (usually MRU) cache, which significantly speeds up 
     * querying. This configuration options defines how small is "small". The default is 10000 - 
     * that is, incidence sets of 10000 or fewer elements are cached in RAM the first time they are
     * accessed.</p>  
     */
    public int getMaxCachedIncidenceSetSize()
    {
        return maxCachedIncidenceSetSize;
    }

    /**
     * <p>Set the configured maximum size of atom incidence sets that are kept in 
     * RAM. Small incidence sets are kept in a (usually MRU) cache, which significantly speeds up 
     * querying. This configuration options defines how small is "small". The default is 10000 - 
     * that is, incidence sets of 10000 or fewer elements are cached in RAM the first time they are
     * accessed.</p>  
     */
    public void setMaxCachedIncidenceSetSize(int maxCachedIncidenceSetSize)
    {
        this.maxCachedIncidenceSetSize = maxCachedIncidenceSetSize;
    }

//    /**
//     * <p>Return <code>true</code> if full (catastrophic) recovery will be run on the storage
//     * layer upon opening the database, and <code>false</code> otherwise.</p>
//     */
//	public boolean isRunDatabaseRecovery()
//	{
//		return runDatabaseRecovery;
//	}
//
//	/**
//     * <p>Specify whether full (catastrophic) recovery should be run on the storage
//     * layer upon opening the database.</p>
//     * 
//     *  @param runDatabaseRecovery - <code>true</code> to run full recovery and
//     *  <code>false</code> not to run it. The default is <code>false</code>. Note that a lightweight
//     *  recovery is run anyway. This flag should be set only if you are not able
//     *  to open the database otherwise. Running a full recovery takes longer than the normal
//     *  recovery.
//	 */
//	public void setRunDatabaseRecovery(boolean runDatabaseRecovery)
//	{
//		this.runDatabaseRecovery = runDatabaseRecovery;
//	}

	/**
	 * Return <code>true</code> (the default) if system-level atom attributes are 
	 * stored and <code>false</code> otherwise. When false, this means that all system
	 * facilities depending on the availability of those attributes are not available.  
	 */
    public boolean isUseSystemAtomAttributes()
    {
        return useSystemAtomAttributes;
    }
    
    /**
     * <p>Return <code>true</code> if the system should detect and throw an exception 
     * when a possible invalid {@link HGAtomRef} occurs. This happens when when an atom
     * is being removed, but here's a {@link HGAtomRef.Mode.hard} reference to it. The
     * default is <code>true</code>. </p> 
     */
    public boolean getPreventDanglingAtomReferences()
	{
		return this.preventDanglingAtomReferences;
	}

    /**
     * <p>Specify whether the system should detect and throw an exception 
     * when a possible invalid {@link HGAtomRef} occurs. This happens when when an atom
     * is being removed, but here's a {@link HGAtomRef.Mode.hard} reference to it. The
     * default is <code>true</code>. </p> 
     */
    public void setPreventDanglingAtomReferences(boolean preventDanglingAtomReferences)
	{
		this.preventDanglingAtomReferences = preventDanglingAtomReferences;
	}

	/**
     * Specify whether system-level atom attributes are 
     * stored - the default is <code>true</code>. When false, this means that all system
     * facilities depending on the availability of those attributes are not available.   
     */
    public void setUseSystemAtomAttributes(boolean useSystemAtomAttributes)
    {
        this.useSystemAtomAttributes = useSystemAtomAttributes;
    }
    
    /**
     * <p>Return the configuration bean for the type system.</p>
     */
    public HGTypeConfiguration getTypeConfiguration()
    {
        return typeConfiguration;
    }

    /**
     * <p>Return the {@link HGEventManager} to be used by the database. The default is an
     * instance of {@link HGDefaultEventManager}.</p>
     */
	public HGEventManager getEventManager()
	{
		return eventManager;
	}

    /**
     * <p>
     * Specify the {@link HGEventManager} to be used by the database. The default is an
     * instance of {@link HGDefaultEventManager}. 
     * </p>
     */
	public void setEventManager(HGEventManager eventManager)
	{
		this.eventManager = eventManager;
	}

	/**
	 * <p>
	 * Return <code>true</code> if links incident to an atom are kept in the database
	 * when that atom is being removed through the {@link HyperGraph.remove(HGHandle)} method
	 * and <code>false</code>otherwise.
	 * </p>
	 */
	public boolean isKeepIncidentLinksOnRemoval()
	{
		return keepIncidentLinksOnRemoval;
	}

	/**
	 * <p>
	 * Specify whether links incident to an atom are kept in the database
	 * when that atom is being removed through the {@link HyperGraph.remove(HGHandle)} method.
	 * </p>
	 */
	public void setKeepIncidentLinksOnRemoval(boolean keepIncidentLinksOnRemoval)
	{
		this.keepIncidentLinksOnRemoval = keepIncidentLinksOnRemoval;
	}	

	/**
	 * <p>Return the configured Java class loader for use by this HyperGraphDB instance or
	 * <code>null</code> if no loader was configured.</p> 
	 */
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    /**
     * <p>
     * Configure a {@link ClassLoader} for this HyperGraphDB instance. The class loader is most
     * importantly used by {@link HGTypeSystem} when loading
     * Java classes based on the <code>classname<->HGDB type</code> mapping. By default, the 
     * type system will use the current thread's context class loader. Setting a custom
     * class loader overrides this behavior. 
     * </p>
     * 
     * @param classLoader
     */
    public void setClassLoader(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    /**
     * <p>Return the {@link HGQueryConfiguration} associated with the {@link HyperGraph}
     * instance.</p>
     */
    public HGQueryConfiguration getQueryConfiguration()
    {
        return queryConfiguration;
    }

    /**
     * <p>Return the cache implementation configured for this {@link HyperGraph} instance.</p> 
     */
    public HGAtomCache getCacheImplementation()
    {
        return cacheImplementation;
    }

    /**
     * <p>
     * Configure a cache implementation to be used for this {@link HyperGraph} instance.
     * </p>
     * @param cacheImplementation
     */
    public void setCacheImplementation(HGAtomCache cacheImplementation)
    {
        this.cacheImplementation = cacheImplementation;
    }    
}