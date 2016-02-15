package org.hypergraphdb.storage;

import java.util.Comparator;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.transaction.HGTransactionFactory;

/**
 * <p>
 * A <code>HGStoreImplementation</code> provides the crucial storage layer for a 
 * HyperGraphDB instance. This interface allows one to plug different storage layers
 * based on different lower-level stores and thus obtain various performance characteristics
 * or transaction guarantees. Implementing this interface is not trivial and some
 * core assumption must be carefully observed. 
 * </p>
 * <p>
 * Note that this interface is an SPI (Service Provider Interface) and it
 * is not intended to be used by an application. Instead, applications should use
 * the {@link org.hypergraphdb.HGStore} class which offers the same functionality, but in
 * the context of a HyperGraphDB instance. All methods here are intended for use
 * by HyperGraphDB itself.
 * </p>
 */
public interface HGStoreImplementation
{
    /**
     * Each different storage mechanism provides its own configuration object
     * with parameters that can be set before storage is initialized. This
     * method returns the storage specific configuration object that must be cast
     * to be appropriate concrete type.
     */
    Object getConfiguration();

    /**
     * <p>
     * Starts the storage engine.
     * </p>
     * <p>
     * This method is invoked after storage configuration parameters have been set
     * and its purpose is to perform complete initialization for the underlying
     * storage layer, possibly creating a new database instance. When this method
     * returns, the storage should be ready for use.
     * </p>
     */
    void startup(HGStore store, HGConfiguration configuration);

    /**
     *
     * <p>
     * Stops the storage engine.
     * </p>
     * <p>
     * This method is invoked when the database instance is closed. It is expected to
     * perform all necessary cleanup and closing of resources of the storage layer.
     * </p>
     */
    void shutdown();

    /**
     * <p> 
     * Return the transaction factory associated with this storage instance. Storage
     * implementations in HyperGraphDB are expected to be transactional and support
     * snapshot isolation semantics. 
     * </p>
     */
    HGTransactionFactory getTransactionFactory();
    
    /**
     * Store a primitive link and return its handle. 
     * 
     * @param handle The handle to use as the identifier of the link.
     * @param link The primitive link. Neither the array nor any of its element can
     * be <code>null</code>.
     * @return The <code>handle</code> argument.
     */
    HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle [] link);

    /**
     * Return the link corresponding to a given handle or <code>null</code> if it is not found.
     */
    HGPersistentHandle [] getLink(HGPersistentHandle handle);

    /**
     * Remove the link corresponding to a given handle.
     */
    void removeLink(HGPersistentHandle handle);

    /**
     * Return <code>true</code> if there is a primitive link for the given handle.
     */
    boolean containsLink(HGPersistentHandle handle);

    /**
     * Store a raw data buffer and return its handle.
     *
     * @param handle The handle to use as identifier within the data space.
     * @param data The buffer to store. There is no limit to the size of this buffer, but it can't be null.
     * @return The <code>handle</code> argument.
     */    
    HGPersistentHandle store(HGPersistentHandle handle, byte [] data);

    /**
     * Return the data corresponding to a given handle or <code>null</code> if it is not found.
     */
    byte [] getData(HGPersistentHandle handle);

    /**
     * Remove the data corresponding to a given handle.
     */
    void removeData(HGPersistentHandle handle);        

    /**
     * Return <code>true</code> if the data space contains data with the given
     * handle and <code>false</code> otherwise.
     */ 
    boolean containsData(HGPersistentHandle handle);
    
    /**
     * Return the incidence set associate with the given handle. The result set should be empty
     * if there is no incidence set for that handle.
     */
    HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle);

    /**
     * Remove the incidence set associated with the given handle. Do nothing if there is no
     * such set.
     */
    void removeIncidenceSet(HGPersistentHandle handle);

    /**
     * Return the number of elements of the incidence set for the given handle or 0
     * if no such set exists.
     */
    long getIncidenceSetCardinality(HGPersistentHandle handle);

    /** 
     * And an element to the incidence set of a given atom (as identified by the <code>handle</code>
     * parameter).
     */
    void addIncidenceLink(HGPersistentHandle handle, HGPersistentHandle newLink);

    /**
     * Remove a specifiec element from the incidence set of a given atom (as identified by
     * the <code>handle</code> parameter).
     */
    void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink);
    

    /**
     * Get the HyperGraphDB index with the given name. Return <code>null</code> if no
     * index with that name exists.
     */
    <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name);

    /**
     * Retrieve the index with the given name, or create a new index if no index with
     * that name exists.
     *
     * @param name The name of the index. It must be unique for the storage layer.
     * @param keyConverter Converter to byte [] for the keys in this index. Necessary when creating
     * a new index. If <code>null</code>, a default converter should be used.
     * @param valueConverter Converter to byte [] for the values  in this index. Necessary when creating
     * a new index. If <code>null</code>, a default converter should be used.
     * @param keyComparator A comparator for the keys in this index. If a non-null comparator is provided, it will be 
     * used by the storage implementation to main the keys in order.  
     * @param valueComparator A comparator for the values in this index. This comparator will be used only when
     * there are multiple values associated with a given key (i.e. "duplicate values"). 
     * @param isBidirectional If <code>true</code> a {@link HGBidirectionalIndex} will be created and 
     * returned, otherwise a normal {@link HGIndex} is returned.
     * @param createIfNecessary Only when this flag is <code>true</code> will an index be created. Otherwise,
     * if this flag is <code>false</code> a no index with the given <code>name</code> exists then
     * <code>null</code> will be returned.
     */
    <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name, 
                                                              ByteArrayConverter<KeyType> keyConverter, 
                                                              ByteArrayConverter<ValueType> valueConverter,
                                                              Comparator<byte[]> keyComparator,
                                                              Comparator<byte[]> valueComparator,
                                                              boolean isBidirectional,
                                                              boolean createIfNecessary);
    /**
     * Remove the index with the given name. Do nothing if not such index exists.
     */
    void removeIndex(String name);    
}