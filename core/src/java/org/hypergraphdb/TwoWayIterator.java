/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.Iterator;

/**
 * <p>
 * This interface defines a bi-directional iterator over a collection of objects. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface TwoWayIterator<T> extends Iterator<T>
{
    /**
     * <p>
     * Return <code>true</code> if there is a previous element in the current
     * iteration state and <code>false</code> otherwise. After the iterator
     * has been initialized, the value of <code>hasPrev</code> will always be
     * <code>false</code>. 
     * </p>
     */
    boolean hasPrev();
    
    /**
     * <p>
     * Returns the previous element in this iteration. 
     * </p>
     * 
     * @throws NoSuchElementException if there is no previous element (e.g. at 
     * the beginning of the iteration).
     */
    T prev();
}
