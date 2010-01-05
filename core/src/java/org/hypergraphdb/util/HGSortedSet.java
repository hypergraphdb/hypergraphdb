/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.SortedSet;

import org.hypergraphdb.HGRandomAccessResult;

/**
 * 
 * <p>
 * A variation of the standard <code>SortedSet</code> interface that offers a
 * <code>HGRandomAccessResult</code> of its elements in addition to an 
 * <code>Iterator</code>. Also, implementations of this interface are guaranteed
 * to be thread-safe. This guarantee has a little twist: if you call <code>iterator</code>,
 * the <code>Iterator</code> returned is not going to be thread-safe and concurrency issues
 * may arise if the set is being modified while the iterator is still in use. However,
 * if you call <code>getSearchResult</code> (note that <code>HGRandomAccessResult</code>
 * extends the <code>Iterator</code> interface), the resulting object will hold
 * a read lock on the set until its <code>close</code> is invoked. This means that any
 * thread trying to modify the set while there's an active search result on it will
 * block, and this includes the thread that opened the search result.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <E>
 */
public interface HGSortedSet<E> extends SortedSet<E>
{
	HGRandomAccessResult<E> getSearchResult();
}
