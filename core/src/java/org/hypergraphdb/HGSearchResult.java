/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.util.CloseMe;

/**
 * <p>
 * Represents the result set of a {@link HyperGraph} query. Note that results are
 * represented as bi-directional iterator with a current position. Moreover, 
 * in general search results must be properly closed as they may hold external system
 * resources such open disk files etc.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGSearchResult<T> extends TwoWayIterator<T>, CloseMe
{
    /**
     * <p>Returns the current element in the result set. If there is no current element,
     * <code>java.util.NoSuchElementException</code> is thrown. There
     * is no current element if the <code>next</code> method was never invoked.
     * </p>
     * 
     * @return The current element in a <code>HGSearchResult</code>.
     * @throw NoSuchElementException if there is no current element.
     */
    T current();
    
    /**
     * <p>Free all system resources held up by the result set and invalidate
     * it for further use.</p> 
     */
    void close();    
    
    /**
     * <p>Return <code>true</code> if the elements in this search result are in ordered
     * and <code>false</code> otherwise. It is assumed that when elements are ordered, 
     * they are instances of <code>java.lang.Comparable</code>
     */
    boolean isOrdered();
    
    /**
     * <p>
     * This object represents an empty <code>HGSearchResult</code>. Calls to
     * <code>hasPrev</code> or <code>hasNext</code> will always return <code>false</code>.
     * Calls to <code>current</code>, <code>next</code> or <code>prev</code> will always
     * throw a <code>NoSuchElementException</code>.  
     * </p>
     */
    public static final HGRandomAccessResult<? extends Object> EMPTY = new EmptySearchResult();
}
