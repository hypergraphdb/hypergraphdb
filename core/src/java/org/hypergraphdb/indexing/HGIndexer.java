/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.indexing;

import java.util.Comparator;


import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * An <code>HGIndexer</code> represents an atom used internally
 * by HyperGraphDB to manage indices. All indexers apply to a 
 * specific atom types.     
 * </p>
 * <p>
 * Because indexers are stored as HyperGraphDB atoms, they must obey
 * the Java Beans conventions of having a default constructor and getter/setter
 * pair for each property that must be recorded into storage.
 * </p>
 * 
 * <p>
 * <strong>IMPORTANT</strong>:Instances of <code>HGIndexer</code> are frequently used to perform lookup on
 * existing indices. For example, when trying to determine whether an atom of 
 * a complex type is being indexed by some projection, one constructs a 
 * <code>ByPartIndexer</code> and performs a lookup in the index manager. Thus, it 
 * is essential that implementation of <code>HGIndexer</code> provide proper <code>hashCode</code>
 * and  <code>equals</code> methods.
 * </p>
 * @author Borislav Iordanov
 *
 */
public interface HGIndexer<KeyType, ValueType>
{
    /**
     * <p>
     * Add a particular atom the the index. This method can safely assume that the atom
     * hasn't been previously added to the index.
     * </p>
     * 
     * @param graph The <code>HyperGraph</code> instance where the atom resides.
     * @param atomHandle
     * @param atom
     * @param index The storage index where the atom should be added.
     */
    void index(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, ValueType> index);
    
    /**
     * <p>
     * Remove a particular from the index, assuming it is currently being indexed.
     * </p>
     * 
     * @param graph The <code>HyperGraph</code> instance where the atom resides.
     * @param atomHandle
     * @param atom
     * @param index The storage index from the atom should be removed.
     */
    void unindex(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, ValueType> index);
    
    /**
     * <p>Return a <code>ByteArrayConverter</code> capable of translating keys
     * returned by this indexer to/from a <code>byte[]</code>.
     *  
     * @param graph The current HyperGraph instance.
     * @return The <code>ByteArrayConverter</code> for type of index keys 
     * return by this indexer or <code>null</code> if keys are of type <code>byte[]</code>. 
     */
    ByteArrayConverter<KeyType> getConverter(HyperGraph graph);
    
    /**
     * <p>
     * Return a comparator used to compare key values return by this indexer.
     * Note that the comparator's <code>compare</code> method will be invoked 
     * with <code>byte[]</code> parameters. It is the comparator's responsibility
     * to convert them to the appropriate run-time type for performing the comparison
     * if need be.
     * </p>
     * 
     * <p>
     * The method may return <code>null</code> if a default byte-by-byte comparator is to be
     * used.
     * </p>
     * 
     * @param graph The current HyperGraph instance.
     * @return A comparator used to compare key values return by this indexer or 
     * <code>null</code> to use a default byte-by-byte comparison of keys. 
     */
    Comparator<byte[]> getComparator(HyperGraph graph);
    
    /**
     * <p>Return the name of the index.</p>  
     */
 //   String getName();
    
    /**
     * <p>Return the handle of the atom type all of whose instances should be indexed
     * by this indexer.</p>  
     */
    HGHandle getType();
    
    /**
     * <p>Set the handle of the atom type all of whose instances should be indexed
     * by this indexer.</p>  
     */
    void setType(HGHandle type);
	
    /**
     * <p>Set the name of the index.</p>  
     */
//    void setName(String name);
	
	/**
	 * <p>Declared to enforce implementation.</p> 
	 */
	int hashCode();
	
	/**
	 * <p>Declared to enforce implementation.</p> 
	 */
	boolean equals(Object other);
}
