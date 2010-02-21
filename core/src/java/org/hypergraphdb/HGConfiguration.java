/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

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
	/**
	 * The default size in bytes of the storage (i.e. BerkeleyDB) cache = 20MB.
	 */
	public static final long DEFAULT_STORE_CACHE = 20*1024*1024; // 20MB
	public static final int  DEFAULT_NUMBER_OF_STORAGE_CACHES = 1;
	
	private boolean transactional;
	private long storeCacheSize = DEFAULT_STORE_CACHE;
	private int numberOfStoreCaches = DEFAULT_NUMBER_OF_STORAGE_CACHES;
	private boolean skipMaintenance = false;
	private boolean cancelMaintenance = false;
	private boolean skipOpenedEvent = false;
	private boolean storageMVCC = true;
	
	public HGConfiguration()
	{
		resetDefaults();
	}
	
	/**
	 * <p>Set all parameters of this configuration to their default values.</p>
	 */
	public void resetDefaults()
	{
		this.transactional = true;
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
	 * 
	 * <p>
	 * Return the size (in bytes) of the cache used by the storage layer. The default value is 
	 * <code>DEFAULT_STORE_CACHE</code>. 
	 * </p>
	 *
	 * @return
	 */
	public long getStoreCacheSize()
	{
		return this.storeCacheSize;
	}
	
	/**
	 * 
	 * <p>
	 * Set the size (in bytes) of the cache used by the storage layer. The default value is
	 * <code>DEFAULT_STORE_CACHE</code>.
	 * </p>
	 *
	 * @param storeCacheSize
	 */
	public void setStoreCacheSize(long storeCacheSize)
	{
		this.storeCacheSize = storeCacheSize;
	}

	/**
	 * <p>Return the number of cache regions of the storage layers. This defaults to 1,
	 * but it may be useful to increase it when the operating system is unable to 
	 * allocate a contiguous memory cache as large as the application needs.
	 * </p> 
	 */
    public int getNumberOfStoreCaches()
    {
        return numberOfStoreCaches;
    }

    /**
     * <p>Specify the number of cache regions of the storage layers. This defaults to 1,
     * but it may be useful to increase it when the operating system is unable to 
     * allocate a contiguous memory cache as large as the application needs.
     * </p> 
     */
    public void setNumberOfStoreCaches(int numberOfStoreCaches)
    {
        this.numberOfStoreCaches = numberOfStoreCaches;
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
     * <p>Return whether MVCC (snapshot) transaction isolation is used by the
     * storage layer. Defaults to <code>true</code> which means less lock contention,
     * higher transaction throughput, but more memory will be used.
     */
    public boolean isStorageMVCC()
    {
        return storageMVCC;
    }

    /**
     * <p>Specify whether MVCC (snapshot) transaction isolation is used by the
     * storage layer. Defaults to <code>true</code> which means less lock contention,
     * higher transaction throughput, but more memory will be used.
     */    
    public void setStorageMVCC(boolean storageMVCC)
    {
        this.storageMVCC = storageMVCC;
    }    
}
