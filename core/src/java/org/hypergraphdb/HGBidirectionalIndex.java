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
 * A <code>HGBidirectionalIndex</code> provides efficient searching of an
 * index entry by value as well as by key. It can be used as an efficient
 * associative array between keys and values.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGBidirectionalIndex<KeyType, ValueType> extends HGIndex<KeyType, ValueType>
{
    /**
     * <p>Return a <code>HGSearchResult</code> over all keys whose
     * value is the <code>value</code> parameter.</p>
     */
	HGRandomAccessResult<KeyType> findByValue(ValueType value);
    
    /**
     * <p>Return a key whose value is the <code>value</code> parameter. If
     * more than one index entry exists with that particular value, generally 
     * the one that was added first will be returned, but this is not guarantueed.
     * </p>
     */
    KeyType findFirstByValue(ValueType value);
    
    /**
     * <p>Return the number of keys pointing to the given values. This operation
     * must execute in constant time, regardless of the data in the index.</p>
     * @param value The value.
     */
    long countKeys(ValueType value);
}
