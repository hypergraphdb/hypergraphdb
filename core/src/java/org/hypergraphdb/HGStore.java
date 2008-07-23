/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import com.sleepycat.db.*;
import java.util.Comparator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.File;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.*;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionBDBImpl;

/**
 * <p>
 * An instance of <code>HGStore</code> is associated with each <code>HyperGraph</code>
 * to manage to low-level interaction with the underlying database mechanism.
 * </p>
 *
 * <p>
 * Normally, the hypergraph store is not accessed directly by applications. However, hypergraph
 * type implementors will rely on the <code>HGStore</code> to manage the way raw data
 * is stored and indexed based on a particular type.
 * </p>
 * 
 * <p>Note that a <code>HGStore</code> does not maintain any data cache, nor does it interact
 * in any special way with the semantic layer of hypergraph and the way data and types are
 * laid out in the store.</p>
 * 
 * @author Borislav Iordanov
 */
public class HGStore
{
	// Initialize the native libraries manually for Windows because of the weird
	// way the OS looks for dependent libraries. We need to load them one by one
	// separately.
	static
	{
		String osname = System.getProperty("os.name");
		if (osname.indexOf("win") > -1 || osname.indexOf("Win") > -1)
		{
			System.loadLibrary("msvcr71");
			System.loadLibrary("msvcp71");			
			System.loadLibrary("libdb44");
			System.loadLibrary("libdb_java44");			
		}
	}
	
    private static final String DATA_DB_NAME = "datadb";
    private static final String PRIMITIVE_DB_NAME = "primitivedb";
    private static final String INCIDENCE_DB_NAME = "incidencedb";
    
    private String databaseLocation;
    
    private Environment env = null;
    private Database data_db = null;
    private Database primitive_db = null;
    private Database incidence_db = null;
    private LinkBinding linkBinding = new LinkBinding();
    private HGTransactionManager transactionManager = null;    
    private HashMap<String, HGIndex<?,?>> openIndices = new HashMap<String, HGIndex<?,?>>();
    private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
    
    private TransactionBDBImpl txn()
    {
    	HGTransaction tx = transactionManager.getContext().getCurrent();;
    	if (tx == null)
    		return TransactionBDBImpl.nullTransaction();
    	else
    		return (TransactionBDBImpl)tx;
    }    
    
    /**
     * <p>Construct a <code>HGStore</code> bound to a specific database 
     * location.</p>
     * 
     * @param database
     */
    public HGStore(String database, HGConfiguration config)
    {
        databaseLocation = database;
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setInitializeCache(true);  
        //envConfig.setCacheSize(config.getStoreCacheSize());
        envConfig.setCacheSize(20*1014*1024);
        envConfig.setCacheCount(50);
        envConfig.setErrorPrefix("BERKELEYDB");
        envConfig.setErrorStream(System.out);
        if (config.isTransactional())
        {
	        envConfig.setInitializeLocking(true);
	        envConfig.setInitializeLogging(true);
	        envConfig.setTransactional(true);
	        envConfig.setTxnWriteNoSync(true);
	        envConfig.setLockDetectMode(LockDetectMode.RANDOM);
	        envConfig.setRunRecovery(true);
	        envConfig.setRegister(true);
	        envConfig.setLogAutoRemove(true);
	        envConfig.setMaxLockers(1000);
	        envConfig.setMaxLockObjects(100000);
	        envConfig.setMaxLocks(100000);
	//        envConfig.setRunFatalRecovery(true);	        
        }
        
        File envDir = new File(databaseLocation);
        envDir.mkdirs();
        try
        {
            env = new Environment(envDir, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            if (env.getConfig().getTransactional())
            	dbConfig.setTransactional(true);
            dbConfig.setType(DatabaseType.BTREE);
            data_db = env.openDatabase(null, DATA_DB_NAME, null, dbConfig);    
            primitive_db = env.openDatabase(null, PRIMITIVE_DB_NAME, null, dbConfig);
            
            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            if (env.getConfig().getTransactional())
            	dbConfig.setTransactional(true);
            dbConfig.setSortedDuplicates(true);
            dbConfig.setType(DatabaseType.BTREE);
            incidence_db = env.openDatabase(null, INCIDENCE_DB_NAME, null, dbConfig);
            
	        transactionManager = new HGTransactionManager(getTransactionFactory());
	        if (!env.getConfig().getTransactional())
	        	transactionManager.disable();
	        else
	        {
	        	final Environment fenv = env;
		        Thread checkPointThread = new Thread(new Runnable()
		        {
		        	public void run()
		        	{
		        		try
		        		{
		        			while (true)
		        			{
		        				Thread.sleep(30000);
		        				env.checkpoint(null);
		        			}
		        		}
		        		catch (Throwable t)
		        		{
		        			System.err.println("HGDB CHECKPOINT THREAD exiting with: " + t.toString());
		        		}
		        	}
		        });
	        	checkPointThread.setName("HGCHECKPOINT");
	        	checkPointThread.setDaemon(true);
	        	checkPointThread.start();
	        }
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to initialize HyperGraph data store: " + ex.toString(), ex);
        }
    }

    /**
     * <p>Create and return a transaction factory for this <code>HGStore</code>.</p>
     */
    public HGTransactionFactory getTransactionFactory()
    {
    	return new HGTransactionFactory()
    	{
    		public HGTransaction createTransaction(HGTransaction parent)
    		{   		
    			try
    			{
	    			TransactionConfig tconfig = new TransactionConfig();
//	    			tconfig.setNoSync(true);
	    			if (parent != null)
	    				return new TransactionBDBImpl(env.beginTransaction(((TransactionBDBImpl)parent).getBDBTransaction(), tconfig));
	    			else
	    				return new TransactionBDBImpl(env.beginTransaction(null, tconfig)); 
    			}
    			catch (DatabaseException ex)
    			{
    				throw new HGException("Failed to create BerkeleyDB transaction object.", ex);
    			}
    		}
    	};
    }
    
    /**
     * <p>Return this store's <code>HGTransactionManager</code>.</p>
     */    
    public HGTransactionManager getTransactionManager()
    {
    	return transactionManager;
    }
    
    /**
     * <p>Return the physical, filesystem location of the HyperGraph store.</p>  
     */
    public String getDatabaseLocation()
    {
    	return this.databaseLocation;
    }
        
    /**
     * <p>Create a new link in the hypergraph store. A new <code>HGPersistentHandle</code>
     * is created to refer to the link.</p>
     * 
     * @param link A non-null, but possibly empty array of persistent atom handles that
     * constitute the link to be created.  
     * @return The newly created <code>HGPersistentHandle</code>.
     */
    public HGPersistentHandle store(HGPersistentHandle [] link)
    {
        return store(HGHandleFactory.makeHandle(), link);
    }
    
    /**
     * <p>Create a new link in the hypergraph store with an existing handle. It is up
     * to the caller of this method to ensure that the passed in handle is unique.</p> 
     * 
     * @param handle A unique <code>HGPersistentHandle</code> that will refer to the link
     * within the hypergraph store.
     * @param link A non-null, but possibly empty array of persistent atom handles that
     * constitute the link to be created. 
     * @param The <code>handle</code> parameter. 
     */
    public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle [] link)
    {
        DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
        DatabaseEntry value = new DatabaseEntry(); 
        linkBinding.objectToEntry(link, value);
        try
        {
            OperationStatus result = data_db.put(txn().getBDBTransaction(), key, value);
            if (result != OperationStatus.SUCCESS)
                throw new Exception("OperationStatus: " + result);            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to store hypergraph link: " + ex.toString(), ex);
        }
        return handle;
    }
    
    /**
     * <p>Write raw binary data to the store. A new persistent handle is created to
     * refer to the data.</p>
     * 
     * @param A non-null, but possibly empty <code>byte[]</code> holding the data to write.
     * @return The newly created <code>HGPersistentHandle</code> that refers to the recorded
     * data.
     */
    public HGPersistentHandle store(byte [] data)
    {
        UUIDPersistentHandle handle = UUIDPersistentHandle.makeHandle();   
        try
        {
            OperationStatus result = primitive_db.put(txn().getBDBTransaction(), 
            									 new DatabaseEntry(handle.toByteArray()), 
            									 new DatabaseEntry(data));
            if (result != OperationStatus.SUCCESS)
                throw new Exception("OperationStatus: " + result);            
            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to store hypergraph raw byte []: " + ex.toString(), ex);
        }
        return handle;
    }
    
    /**
     * <p>Write raw binary data to the store using a pre-created, unique persistent handle.</p>
     * 
     * @param handle A unique <code>HGPersistentHandle</code> to be recorded as the data key.
     * @param A non-null, but possibly empty <code>byte[]</code> holding the data to write.
     * @return The <code>handle</code> parameter.
     */    
    public void store(HGPersistentHandle handle, byte [] data)
    {
        try
        {
            OperationStatus result = primitive_db.put(txn().getBDBTransaction(), 
            									 new DatabaseEntry(handle.toByteArray()), 
            									 new DatabaseEntry(data));
            if (result != OperationStatus.SUCCESS)
                throw new Exception("OperationStatus: " + result);            
            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to store hypergraph raw byte []: " + ex.toString(), ex);
        }        
    }
    
    /**
     * <p>Remove the value associated with a <code>HGPersistentHandle</code> key. The value can
     * be either a link or raw data. Note that this is a more expensive operation than either
     * of <code>removeLink</code> or <code>removeData</code>.</p> 
     */
/*    public void remove(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            data_db.delete(txn(), key);
            primitive_db.delete(txn(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove value with handle " + handle + 
                    ": " + ex.toString(), ex);            
        }
    } */
    
    /**
     * <p>Remove a link value associated with a <code>HGPersistentHandle</code> key.</p> 
     */    
    public void removeLink(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            data_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove value with handle " + handle + 
                    ": " + ex.toString(), ex);            
        }
    }

    /**
     * <p>Remove a raw data value associated with a <code>HGPersistentHandle</code> key.</p> 
     */
    public void removeData(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            primitive_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove value with handle " + handle + 
                    ": " + ex.toString(), ex);            
        }
    }
    
    /**
     * <p>Retrieve an existing link by its handle.</p>
     * 
     * @param handle The persistent handle of the link. A <code>NullPointerException</code> is
     * thrown if this parameter is <code>null</code>. 
     * @return An array of handles forming the link or <code>null</code> if there is no
     * link with that <code>HGPersistentHandle</code> in the database. Note that if the passed
     * in handle points, the behavior is undefined - the method might throw an exception or return
     * an array of invalid links.
     */
    public HGPersistentHandle [] getLink(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getLink called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            if (data_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
                return (HGPersistentHandle [])linkBinding.entryToObject(value);
            else
                return null;
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve link with handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
    }

    /**
     * <p>
     * Retrieves an existing link in raw byte form. The returned byte array contains 
     * the 16 byte UUID of the handles constituting the link.
     * </p>
     * 
     * @param handle
     * @return
     */
    public byte [] getLinkData(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getLink called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            if (data_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
                return value.getData();
            else
                return null;
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve link with handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
    }
    
    /**
     * <p>
     * Read a persistent handle array of size <code>n</code> out of a raw data buffer.
     * The buffer must contain at least <code>n</code> persistent handles starting 
     * at <code>offset</code>.
     * </p>
     * 
     * @param data The data buffer.
     * @param offset The 0 based offset from which the read starts.
     * @param n The number of handles to read.
     * @return A new <code>HGPersistentHandle[]</code> with the retrieved handles.
     */
    public HGPersistentHandle [] readNHandles(byte [] data, int offset, int n)
    {
    	return readHandles(data, offset, n * UUIDPersistentHandle.SIZE);
    }
    
    /**
     * <p>
     * Read a persistent handle array of size <code>n</code> out of a raw data buffer.
     * </p>
     *
     * @param data The data buffer.
     * @param offset The 0 based offset from which the read starts.
     * @param length The number of bytes to read. 
     * @return A new <code>HGPersistentHandle[]</code> with the retrieved handles.
     */
    public HGPersistentHandle [] readHandles(byte [] data, int offset, int length)
    {
    	return LinkBinding.readHandles(data, offset, length);
    }
    
    /**
     * <p>
     * Read a persistent handle array of size <code>n</code> out of a raw data buffer.
     * All bytes including and following <code>offset</code> are read. 
     * </p>
     *
     * @param data The data buffer.
     * @param offset The 0 based offset from which the read starts.
     * @return A new <code>HGPersistentHandle[]</code> with the retrieved handles.
     */
    public HGPersistentHandle [] readHandles(byte [] data, int offset)
    {
    	if (data == null)
    		return null;
    	else
    		return readHandles(data, offset, data.length - offset);
    }

    /**
     * <p>Retrieve the raw data buffer stored at <code>handle</code>.</p>
     * 
     * @param handle The <code>HGPersistentHandle</code> of the data. Cannot
     * be <code>null</code>.
     * @return The data pointed to by <code>handle</code> or <code>null</code>
     * if it could not be found.
     */
    public byte [] getData(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getData called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            if (primitive_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
                return value.getData();
            else
                return null;            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve link with handle " + handle + 
                                  ": " + ex.toString(), ex);
        }        
    }
    
    public HGHandle [] getIncidenceSet(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getIncidenceSet called with a null handle.");
        
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();            
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
                return new HGHandle[0];
            HGHandle [] result = new HGHandle[cursor.count()];
            for (int i = 0; status == OperationStatus.SUCCESS; i++)
            {
                result[i] = UUIDPersistentHandle.makeHandle(value.getData());
                status = cursor.getNextDup(key, value, LockMode.DEFAULT);
            }
            return result;
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Exception ex) { ex.printStackTrace(System.err); }
        }
    }

    /**
     * <p>Return a <code>HGSearchResult</code> of atom handles in a given atom's incidence
     * set.</p>
     *  
     * @param handle The <code>HGPersistentHandle</code> of the atom whose incidence set
     * is desired.
     * @return The <code>HGSearchResult</code> iterating over the incidence set. 
     */
    @SuppressWarnings("unchecked")    
    public HGSearchResult<HGHandle> getIncidenceResultSet(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getIncidenceSet called with a null handle.");
        
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            TransactionBDBImpl tx = txn();
            cursor = incidence_db.openCursor(tx.getBDBTransaction(), null);            
            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
            {
            	cursor.close();
                return (HGSearchResult<HGHandle>)HGSearchResult.EMPTY;
            }
            else
            	return new SingleKeyResultSet(tx.attachCursor(cursor), key, BAtoHandle.getInstance());            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
    }
    
    /**
     * <p>Return the number of atoms in the incidence set of a given atoms. That is,
     * return the number of links pointing to the atom.</p>
     */
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getIncidenceSetCardinality called with a null handle.");
        
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();            
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
            	return 0;
            else
            	return cursor.count();
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }    	
        finally
        {
        	try { cursor.close(); } catch (Throwable t) { }
        }
    }
    
    /**
     * <p>Update the incidence set of an atom with a newly created link pointing to it.
     * This method is only to be used internally by hypergraph.
     * </p>
     * 
     * <p>
     * If the link is already part of the incidence set of this atom, it will not be added
     * again.
     * </p>
     * 
     * @param handle The <code>HGPersistentHandle</code> of the atom whose incidence set
     * is to be updated.
     * @param newLink The <code>HGPersistentHandle</code> of the new link pointing to that 
     * atom.
     */
    void addIncidenceLink(HGPersistentHandle handle, HGPersistentHandle newLink)
    {
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry(newLink.toByteArray());
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
            {
                OperationStatus result = incidence_db.put(txn().getBDBTransaction(), key, value);
                if (result != OperationStatus.SUCCESS)
                    throw new Exception("OperationStatus: " + result);
            }
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to update incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Exception ex) { }
        }        
    }

    /**
     * <p>Update the incidence set of an atom by removing a link that no longer points
     * to it. This method is only to be used internally by hypergraph.
     * </p>
     * 
     * <p>
     * If the link is not part of the incidence set of this atom, nothing will be done.
     * </p>
     * 
     * @param handle The <code>HGPersistentHandle</code> of the atom whose incidence set
     * is to be updated.
     * @param oldLink The <code>HGPersistentHandle</code> of the old link that no longer
     * points to that atom.
     */
    void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
    {
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry(oldLink.toByteArray());
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            {
                cursor.delete();
            }
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to update incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Exception ex) { }
        }    
    }
    
    /**
     * <p>Remove the whole incidence set of a given handle. This method is 
     * normally used only when an atom is being removed from the hypergraph DB.</p>
     * 
     * @param handle The handle of the atom whose incidence set must be removed.
     */
    void removeIncidenceSet(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.removeIncidenceSet called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            incidence_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove incidence set of handle " + handle + 
                    ": " + ex.toString(), ex);            
        }        
    }
    
    // ------------------------------------------------------------------------
    // INDEXING
    // ------------------------------------------------------------------------

    boolean checkIndexExisting(String name)
    {
    	if (openIndices.get(name) != null)
    		return true;
    	else
    	{
    		DatabaseConfig cfg = new DatabaseConfig();
    		cfg.setAllowCreate(false);
    		Database db = null;
    		try
    		{
    			db = env.openDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name, null, cfg);
    		}
    		catch (Exception ex)
    		{
    		}
    		if (db != null)    			
    		{
    			try { db.close(); } catch (Throwable t) { t.printStackTrace(); }
    			return true;
    		}
    		else
    			return false;
    	}    	
    }
    
    /**
     * <p>
     * Create a new index with the specified name. If an index with this
     * name already exists, the method will return <code>null</code>. 
     * </p>
     * 
     * <p>
     * Once the index is created, it can be used without further setup. Note that
     * the <code>HGStore</code> does not provide any automatic population of manually
     * created indices. It does, however, manage entries once they are added to an index 
     * so that integrity is maintained after a removal operation.
     * </p>
     * 
     * @param name The name of the newly created index.
     * @param comparatorClass The comparator class used to compare the keys of this index. This
     * parameter may be <code>null</code> if the default, lexicographical byte ordering
     * comparator is to be used. 
     * @return A ready to use <code>HGIndex</code> or <code>null</code> if an
     * index with the specified name already exists.
     */   
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> createIndex(String name,
    																	ByteArrayConverter<KeyType> keyConverter,
    																	ByteArrayConverter<ValueType> valueConverter,
    																	Comparator<?> comparator)
    {
    	indicesLock.writeLock().lock();
    	try
    	{
	    	if (checkIndexExisting(name))
	    		return null;
	    	DefaultIndexImpl<KeyType, ValueType> idx = 
	    		new DefaultIndexImpl<KeyType, ValueType>(name, 
	    												 env, 
	    												 transactionManager,
	    												 keyConverter, 
	    												 valueConverter,
	    												 comparator);
	    	idx.open();
	    	openIndices.put(name, idx);
	    	return idx;
    	}
    	finally
    	{
    		indicesLock.writeLock().unlock();
    	}
    }
     
    /**
     * <p>
     * Creates a new <code>HGBidirectionalIndex</code>. This method has the exact
     * same behavior as the <code>createIndex</code> method, except that a 
     * bidirectional implementation is constructed.
     * </p>
     * 
     */
    @SuppressWarnings("unchecked")
    public <KeyType, ValueType> HGBidirectionalIndex<KeyType, ValueType> 
        createBidirectionalIndex(String name, 
        						 ByteArrayConverter<KeyType> keyConverter, 
        						 ByteArrayConverter<ValueType> valueConverter,
        						 Comparator comparator)
    {
    	indicesLock.writeLock().lock();
    	try
    	{
	    	if (checkIndexExisting(name))
	    		return null;
	    	DefaultBiIndexImpl<KeyType, ValueType> idx = 
	    		new DefaultBiIndexImpl<KeyType, ValueType>(name, 
	    												   env, 
	    												   transactionManager,
	    												   keyConverter, 
	    												   valueConverter,
	    												   comparator);
	    	idx.open();    	
	    	openIndices.put(name, idx);
	    	return idx;
    	}
    	finally
    	{
    		indicesLock.writeLock().unlock();
    	}
    }
    
    /**
     * <p>
     * Retrieve an <code>HGIndex</code> by its name. An index will not 
     * be automatically created if it does not exists. To create an index,
     * use the <code>createIndex</code> method.
     * </p>
     * 
     * @param name The name of the desired index.
     * @return The <code>HGIndex</code> with the given name or <code>null</code>
     * if no such index exists.
     */
    @SuppressWarnings("unchecked")
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name, 
																	 ByteArrayConverter<KeyType> keyConverter, 
																	 ByteArrayConverter<ValueType> valueConverter,
																	 Comparator comparator)
    {
    	indicesLock.readLock().lock();
    	try
    	{
	    	HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
	    	if (idx != null)
	    		return idx;
	    	if (!checkIndexExisting(name))
	    		return null;
    	}
    	finally {indicesLock.readLock().unlock(); }
    	
    	indicesLock.writeLock().lock();
    	try
    	{
	    	HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
	    	if (idx != null)
	    		return idx;
	    	if (!checkIndexExisting(name))
	    		return null;
    		
			DefaultIndexImpl<KeyType, ValueType> result = 
	    		new DefaultIndexImpl<KeyType, ValueType>(name, 
	    												 env, 
	    												 transactionManager,
	    												 keyConverter, 
	    												 valueConverter,
	    												 comparator);
	    	result.open();    		
	    	openIndices.put(name, result);
	    	return result;
    	}
    	finally
    	{
    		indicesLock.writeLock().unlock();
    	}
    }
    
    /**
     * <p>Retrieve an existing <code>HGBidirectionalIndex</code> by its name.</p>
     * 
     *  @throws ClassCastException is the index with the specified name is not
     *  bidirectional.
     */
    @SuppressWarnings("unchecked")    
    public <KeyType, ValueType> HGBidirectionalIndex<KeyType, ValueType> getBidirectionalIndex(String name,
																							   ByteArrayConverter<KeyType> keyConverter, 
																							   ByteArrayConverter<ValueType> valueConverter,
																							   Comparator comparator)
    {
    	indicesLock.readLock().lock();
    	try
    	{
	    	HGBidirectionalIndex<KeyType, ValueType> idx = (HGBidirectionalIndex<KeyType, ValueType>)openIndices.get(name);
	    	if (idx != null)
	    		return idx;
	    	if (!checkIndexExisting(name))
	    		return null;
    	}    	
    	finally
    	{
    		indicesLock.readLock().unlock();
    	}
    	
    	indicesLock.writeLock().lock();
    	try
    	{    		
    		DefaultBiIndexImpl<KeyType, ValueType> result = 
        		new DefaultBiIndexImpl<KeyType, ValueType>(name, 
        					  							   env, 
        					  							   transactionManager,
        												   keyConverter, 
        												   valueConverter,
        												   comparator);
        	result.open();    		
        	openIndices.put(name, result);
        	return result;
    	}
    	finally
    	{
    		indicesLock.writeLock().unlock();
    	}
    }
    
    /**
     * <p>
     * Remove an index from the database. Note that all entries in this index will 
     * be lost.
     * </p>
     */
    @SuppressWarnings("unchecked")    
    public void removeIndex(String name)
    {
    	indicesLock.writeLock().lock();
    	try
    	{
	    	HGIndex idx = openIndices.get(name);
	    	if (idx != null)
	    	{
	    		idx.close();
	    		openIndices.remove(name);
	    	}
	    	try
	    	{
	    		env.removeDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name, null);
	    	}
	    	catch (Exception e)
	    	{
	    		throw new HGException(e);
	    	}
    	}
    	finally
    	{
    		indicesLock.writeLock().unlock();
    	}
    }
    
    @SuppressWarnings("unchecked")    
    public void close()
    {
        if (env != null)
        {
        	try
        	{
        		if (env.getConfig().getTransactional())
        			env.checkpoint(null);
        	}
        	catch (Throwable t)
        	{
        		t.printStackTrace();
        	}
       	
            //
            // Close all indices
            //
            for (Iterator<HGIndex<?,?>> i = openIndices.values().iterator(); i.hasNext(); )
                try
                {
                	i.next().close();
                }
                catch (Throwable t)
                {
                    // TODO - we need to log the exception here, once we've decided
                    // on a logging mechanism. 
                	t.printStackTrace();
                }
            try { data_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { primitive_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { incidence_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { env.close(); }
            catch (Throwable t) { t.printStackTrace(); }
        } 
   	}
}