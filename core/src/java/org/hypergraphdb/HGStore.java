/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.Comparator;

import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.BDBStorageImplementation;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.storage.LinkBinding;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.HGTransactionManager;

/**
 * <p>
 * An instance of <code>HGStore</code> is associated with each <code>HyperGraph</code>
 * to manage to low-level interaction with the underlying database mechanism.
 * </p>
 *
 * <p>
 * Normally, the HyperGraphDB store is not accessed directly by applications. However, HyperGraphDB
 * type implementors will rely on the <code>HGStore</code> to manage the way raw data
 * is stored and indexed based on a particular type.
 * </p>
 * 
 * <p>Note that a <code>HGStore</code> does not maintain any data cache, nor does it interact
 * in any special way with the semantic layer of HyperGraphDB and the way data and types are
 * laid out in the store.</p>
 * 
 * @author Borislav Iordanov
 */
public class HGStore
{
    private String databaseLocation;
//    private HGConfiguration config;
    private HGTransactionManager transactionManager = null;    
    private HGStoreImplementation impl = new BDBStorageImplementation();    
    
    private ThreadLocal<StorageGraph> overlayGraph = new ThreadLocal<StorageGraph>();
      
    /**
     * <p>Construct a <code>HGStore</code> bound to a specific database 
     * location.</p>
     * 
     * @param database
     */
    public HGStore(String database, HGConfiguration config)
    {
        databaseLocation = database;
//        this.config = config;
        impl.startup(this, config);
        transactionManager = new HGTransactionManager(impl.getTransactionFactory());
        if (!config.isTransactional())
            transactionManager.disable();        
    }
    
//    DefaultIndexImpl<HGPersistentHandle, HGPersistentHandle> getIncidenceDbAsIndex()
//    {
//    	return new DefaultIndexImpl<HGPersistentHandle, HGPersistentHandle>(
//    			env,
//    			incidence_db,
//			    transactionManager,
//				BAtoHandle.getInstance(), 
//				BAtoHandle.getInstance(),
//				null);
//    }
//    
    /**
     * <p>Create and return a transaction factory for this <code>HGStore</code>.</p>
     */
    public HGTransactionFactory getTransactionFactory()
    {
        return impl.getTransactionFactory();
    }
    
    /**
     * <p>Return this store's <code>HGTransactionManager</code>.</p>
     */    
    public HGTransactionManager getTransactionManager()
    {
    	return transactionManager;
    }
    
    /**
     * <p>Return the physical, file system location of the HyperGraph store.</p>  
     */
    public String getDatabaseLocation()
    {
    	return this.databaseLocation;
    }
        
    /**
     * <p>Create a new link in the HyperGraphDB store. A new <code>HGPersistentHandle</code>
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
     * <p>Create a new link in the HyperGraphDB store with an existing handle. It is up
     * to the caller of this method to ensure that the passed in handle is unique.</p> 
     * 
     * @param handle A unique <code>HGPersistentHandle</code> that will refer to the link
     * within the HyperGraphDB store.
     * @param link A non-null, but possibly empty array of persistent atom handles that
     * constitute the link to be created. 
     * @param The <code>handle</code> parameter. 
     */
    public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle [] link)
    {
        return impl.store(handle, link);      
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
        store(handle, data);
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
        impl.store(handle, data);        
    }
    
    /**
     * <p>Remove a link value associated with a <code>HGPersistentHandle</code> key.</p> 
     */    
    public void removeLink(HGPersistentHandle handle)
    {
        impl.removeLink(handle);
    }

    /**
     * <p>Remove a raw data value associated with a <code>HGPersistentHandle</code> key.</p> 
     */
    public void removeData(HGPersistentHandle handle)
    {
        impl.removeData(handle);
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
        if (overlayGraph.get() != null)
        {
            HGPersistentHandle [] result = null;                
            if ( (result = overlayGraph.get().getLink(handle)) != null)
                return result;
        }         
        return impl.getLink(handle);
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
//    public byte [] getLinkData(HGPersistentHandle handle)
//    {
//        if (handle == null)
//            throw new NullPointerException("HGStore.getLink called with a null handle.");
//        try
//        {
//            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
//            DatabaseEntry value = new DatabaseEntry();
//            if (data_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
//                return value.getData();
//            else
//                return null;
//        }
//        catch (Exception ex)
//        {
//            throw new HGException("Failed to retrieve link with handle " + handle, ex);
//        }
//    }
    
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

    public boolean containsLink(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.getLink called with a null handle.");
        if (overlayGraph.get() != null && overlayGraph.get().getLink(handle) != null)
                return true;
        return impl.containsLink(handle);
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
        if (overlayGraph.get() != null)
        {
            byte [] result = null;                
            if ( (result = overlayGraph.get().getData(handle)) != null)
                return result;
        }
        return impl.getData(handle);
    }
    
//    public HGPersistentHandle [] getIncidenceSet(HGPersistentHandle handle)
//    {
//        if (handle == null)
//            throw new NullPointerException("HGStore.getIncidenceSet called with a null handle.");
//        
//        Cursor cursor = null;
//        try
//        {
//            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
//            DatabaseEntry value = new DatabaseEntry();            
//            cursor = incidence_db.openCursor(txn().getBDBTransaction(), cursorConfig);
//            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
//            if (status == OperationStatus.NOTFOUND)
//                return new HGPersistentHandle[0];
//            HGPersistentHandle [] result = new HGPersistentHandle[cursor.count()];
//            for (int i = 0; status == OperationStatus.SUCCESS; i++)
//            {
//                result[i] = UUIDPersistentHandle.makeHandle(value.getData());
//                status = cursor.getNextDup(key, value, LockMode.DEFAULT);
//            }
//            return result;
//        }
//        catch (Exception ex)
//        {
//            throw new HGException("Failed to retrieve incidence set for handle " + handle, ex);
//        }
//        finally
//        {
//            if (cursor != null)
//                try { cursor.close(); } catch (Exception ex) { ex.printStackTrace(System.err); }
//        }
//    }

    /**
     * <p>Return a <code>HGSearchResult</code> of atom handles in a given atom's incidence
     * set.</p>
     *  
     * @param handle The <code>HGPersistentHandle</code> of the atom whose incidence set
     * is desired.
     * @return The <code>HGSearchResult</code> iterating over the incidence set. 
     */    
    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle)
    {
        return impl.getIncidenceResultSet(handle);
    }
    
    /**
     * <p>Return the number of atoms in the incidence set of a given atoms. That is,
     * return the number of links pointing to the atom.</p>
     */
    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        return impl.getIncidenceSetCardinality(handle);
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
    public void addIncidenceLink(HGPersistentHandle handle, HGPersistentHandle newLink)
    {
        impl.addIncidenceLink(handle, newLink);
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
    public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
    {
        impl.removeIncidenceLink(handle, oldLink);
    }
    
    /**
     * <p>Remove the whole incidence set of a given handle. This method is 
     * normally used only when an atom is being removed from the hypergraph DB.</p>
     * 
     * @param handle The handle of the atom whose incidence set must be removed.
     */
    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.removeIncidenceSet called with a null handle.");
        impl.removeIncidenceSet(handle);
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
//    public <KeyType, ValueType> HGIndex<KeyType, ValueType> createIndex(String name,
//    																	ByteArrayConverter<KeyType> keyConverter,
//    																	ByteArrayConverter<ValueType> valueConverter,
//    																	Comparator<?> comparator)
//    {
//    	indicesLock.writeLock().lock();
//    	try
//    	{
//	    	if (checkIndexExisting(name))
//	    		return null;
//	    	DefaultIndexImpl<KeyType, ValueType> idx = 
//	    		new DefaultIndexImpl<KeyType, ValueType>(name, 
//	    												 env, 
//	    												 transactionManager,
//	    												 keyConverter, 
//	    												 valueConverter,
//	    												 comparator);
//	    	idx.open();
//	    	openIndices.put(name, idx);
//	    	return idx;
//    	}
//    	finally
//    	{
//    		indicesLock.writeLock().unlock();
//    	}
//    }
     
    /**
     * <p>
     * Creates a new <code>HGBidirectionalIndex</code>. This method has the exact
     * same behavior as the <code>createIndex</code> method, except that a 
     * bidirectional implementation is constructed.
     * </p>
     * 
     */
    @SuppressWarnings("unchecked")
//    public <KeyType, ValueType> HGBidirectionalIndex<KeyType, ValueType> 
//        createBidirectionalIndex(String name, 
//        						 ByteArrayConverter<KeyType> keyConverter, 
//        						 ByteArrayConverter<ValueType> valueConverter,
//        						 Comparator comparator)
//    {
//    	indicesLock.writeLock().lock();
//    	try
//    	{
//	    	if (checkIndexExisting(name))
//	    		return null;
//	    	DefaultBiIndexImpl<KeyType, ValueType> idx = 
//	    		new DefaultBiIndexImpl<KeyType, ValueType>(name, 
//	    												   env, 
//	    												   transactionManager,
//	    												   keyConverter, 
//	    												   valueConverter,
//	    												   comparator);
//	    	idx.open();    	
//	    	openIndices.put(name, idx);
//	    	return idx;
//    	}
//    	finally
//    	{
//    		indicesLock.writeLock().unlock();
//    	}
//    }
//    
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
//    @SuppressWarnings("unchecked")
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name, 
																	 ByteArrayConverter<KeyType> keyConverter, 
																	 ByteArrayConverter<ValueType> valueConverter,
																	 Comparator comparator)
    {
        return impl.getIndex(name, keyConverter, valueConverter, comparator, false, true);
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
        return (HGBidirectionalIndex<KeyType, ValueType>)
            impl.getIndex(name, keyConverter, valueConverter, comparator, true, true);
        
    }
    
    /**
     * <p>
     * Remove an index from the database. Note that all entries in this index will 
     * be lost.
     * </p>
     */
    public void removeIndex(String name)
    {
        impl.removeIndex(name);
    }
        
    public void close()
    {
        impl.shutdown();
   	}
    
    
    public void attachOverlayGraph(StorageGraph sgraph)
    {
        overlayGraph.set(sgraph);
    }
    
    public void detachOverlayGraph()
    {
        overlayGraph.set(null);
    }
}