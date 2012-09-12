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
 * The <code>HGSearchable</code> interface specifies that an object can be searched by a key.
 * Things <code>HGSearchable</code> in HyperGraph are indexes, certain types and the like.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGSearchable<KeyType, ValueType>
{
    /**
     * <p>
     * Returns a <code>HGSearchResult</code> over all values matching a key in the 
     * searched entity.</p>
     * 
     * <p>If there are no matches, the method should return <code>HGSearchResult.EMPTY</code>. The
     * method will never return a <code>null</code>. A <code>HGException</code> may
     * be thrown in exceptional situations.</p>
     */
    HGSearchResult<ValueType> find(KeyType key);
}
