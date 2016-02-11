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
 * A <code>HGSortIndex</code> is a <code>HGIndex</code> that maintains its
 * keys in an order relation. It is therefore an <code>HGOrderedSearchable</code>
 * entity.
 * </p>
 * <p>
 *  Note that the order of
 *  values returned by all methods in this interface is arbitrary even if there is a natural ordering defined for them. 
 *  The values may come in ordered batches, one per key, since usually scanning over a range of key/value pairs staying
 *  on the same key for all sorted values, but this interface mandates no
 *  such guarantee.
 * </p>
 * @author Borislav Iordanov
 */
public interface HGSortIndex<KeyType, ValueType> extends HGIndex<KeyType, ValueType>, 
														 HGOrderedSearchable<KeyType, ValueType>
{
	/**
	 * Return a result set over all values of all keys less than the specified key. 
	 */
	HGSearchResult<ValueType> findLT(KeyType key);
	/**
	 * Return a result set over all values of all keys greater than the specified key. 
	 */
	HGSearchResult<ValueType> findGT(KeyType key);
	/**
	 * Return a result set over all values of all keys less than or equal to the specified key. 
	 */	
	HGSearchResult<ValueType> findLTE(KeyType key);
	/**
	 * Return a result set over all values of all keys greater than or equal to the specified key. 
	 */	
	HGSearchResult<ValueType> findGTE(KeyType key);	
}