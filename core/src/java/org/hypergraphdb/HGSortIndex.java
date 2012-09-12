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
 * 
 * <p>
 * Taking advantage of the new Java 1.5 feature allowing overriding methods to
 * further specialize on the return type (i.e. allowing contravariant return types),
 * all lookup methods of the super-interfaces are redeclared to return a 
 * <code>HGRandomAccessResult</code>. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGSortIndex<KeyType, ValueType> extends HGIndex<KeyType, ValueType>, 
														 HGOrderedSearchable<KeyType, ValueType>
{
	HGSearchResult<ValueType> findLT(KeyType key);
	HGSearchResult<ValueType> findGT(KeyType key);
	HGSearchResult<ValueType> findLTE(KeyType key);
	HGSearchResult<ValueType> findGTE(KeyType key);	
}
