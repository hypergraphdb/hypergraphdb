/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;


/**
 * <p>
 * The <code>HGOrderedSearchable</code> interface specifies that an object can be 
 * viewed as an ordered collection from where a range of values can be obtained 
 * based on a key and comparison operator.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGOrderedSearchable<KeyType, ValueType> extends HGSearchable<KeyType, ValueType>
{
    /**
     * <p>Return a range of all values <em>strictly less than</em>
     * the specified key.</p>
     * 
     * @param key The search key.
     * @return An <code>HGSearchResult</code> over the resulting range of values.
     */
    HGSearchResult<ValueType> findLT(KeyType key);

    /**
     * <p>Return a range of all values <em>strictly greater than</em>
     * the specified key.</p>
     * 
     * @param key The search key.
     * @return An <code>HGSearchResult</code> over the resulting range of values.
     */    
    HGSearchResult<ValueType> findGT(KeyType key);
    
    /**
     * <p>Return a range of all values <em>less than or equal to</em>
     * the specified key.</p>
     * 
     * @param key The search key.
     * @return An <code>HGSearchResult</code> over the resulting range of values.
     */    
    HGSearchResult<ValueType> findLTE(KeyType key);
    
    /**
     * <p>Return a range of all values <em>greater than or equal to</em>
     * the specified key.</p>
     * 
     * @param key The search key.
     * @return An <code>HGSearchResult</code> over the resulting range of values.
     */    
    HGSearchResult<ValueType> findGTE(KeyType key);
}
