/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;


/**
 * <p>The <code>HGIndex</code> interface represents an user-created index in the HyperGraph 
 * data structure.</p>
 * 
 * <p>
 * Note that taking advantage of the fact that Java allows overriding methods to
 * further specialize on the return type (i.e. allowing contra-variant return types),
 * the <code>find</code> method of the super-interface is redeclared to return a 
 * <code>HGRandomAccessResult</code>. 
 * </p> 
 * 
 * @author Borislav Iordanov
 */
public interface HGIndex<KeyType, ValueType> extends HGSearchable<KeyType, ValueType>
{    
    /**
     * Return the unique name that identifies this index at the storage layer.
     */
    public String getName();

    /**
     * <p>
     * Add an entry to the index. If that entry is already present, calling this
     * method should have no effect.
     * </p>
     * 
     * @param key The entry's key part.
     * @param value The entry's value part.
     */
    void addEntry(KeyType key, ValueType value);

    /**
     * <p>Remove a specific entry in the index. If an entry
     * with this key and value does not exist, the method does not nothing.
     * </p>
     * 
     * @param key The key part of the entry.
     * @param value The value part of the entry.
     */
    void removeEntry(KeyType key, ValueType value);

    /**
     * <p>Remove all entries in the index with a given key. If an entry
     * with this key does not exist, the method does not nothing.
     * </p>
     * 
     * @param key The key all of whose corresponding entries will be removed.
     */
    void removeAllEntries(KeyType key);
    
    /**
     * <p>
     * Find the first indexed entry corresponding to the given key. The first
     * entry will generally be the one that was firstly added for that key. However,
     * this is by no means guaranteed. This method is meant for indices where
     * only a single value corresponds to a key. That is, in mathematical terms, indices
     * that can be seen as functions.
     * </p>
     * 
     * @param key The key whose value is sought.
     * @return The first entry for that key.
     */
    ValueType findFirst(KeyType key);
    
    /**
     * <p>
     * Retrieve all entries corresponding to the given key. The order in 
     * which the entries are returned is not necessarily the order in 
     * which they were originally added.
     * </p>
     * 
     * @param key The key whose values are sought.
     * @return A <code>HGRandomAccessResult</code> over all <code>HGPersistentHandle</code>s under that key.
     */
    HGRandomAccessResult<ValueType> find(KeyType key);
    
    /**
     * <p>
     * Open the index for use. Entries may be added to the index only when it
     * has been explicitly opened. To determine whether an index is currently
     * opened, use the <code>isOpen</code> method. Note that an index may be
     * temporarily opened by the HyperGraph querying mechanism.
     * </p> 
     */
    public void open();
    
    /**
     * <p>
     * Close this index. This method closes any run-time resources associated with
     * the index, and invalidates it for use until reopened. It does not remove
     * the index permanently from the database.
     * </p> 
     */
    void close();
    
    /**
     * <p>Return <code>true</code> if the index is currently opened and
     * <code>false</code> otherwise.
     * </p> 
     */
    boolean isOpen();
    
    /**
     * <p>Return a result set containing all keys in this index.</p>
     */
    HGRandomAccessResult<KeyType> scanKeys();
    
    /**
     * <p>Return a result set containing all values in this index.</p>
     */
    HGRandomAccessResult<ValueType> scanValues();
    
    /**
     * <p>Return the number of keys in this index. This operation must run
     * in constant time, regardless of the number of keys.</p>
     */
    long count();

    /**
     * <p>Return the number of values for the key. This operation must run 
     * constant time regardless of the key or the number returned.</p>
     * @param key The key whose values must be counted.
     */
    long count(KeyType key);
}
