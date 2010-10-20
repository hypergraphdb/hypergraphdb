/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * This interface represents a generic graph traversal. Concrete traversals, reflecting
 * variations in the order of visiting and of the filtering of the nodes are implemented
 * to suit various graph related algorithms.  
 * </p>
 * 
 * <p>
 * Traversal can be used either in the implementation graph algorithms, such as finding the
 * shortest path between two atoms, or as the underlying result sets of certain queries. Because
 * of this commonality, traversals are driven "by the outside", instead of doing the actual work
 * of visiting through a callback mechanism. Nevertheless, concrete traversals may expose some
 * form of callbacks for various events during the traversal, or expose the internal state 
 * of the process.
 * </p>
 *
 * <p>
 * It should be quite possible to implement the traversals as bidirectional (i.e. with
 * <code>hasPrev</code> and <code>prev</code> methods defined) should a real need arise.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGTraversal extends java.util.Iterator<Pair<HGHandle, HGHandle>> 
{
	/**
	 * <p>Return <code>true</code> if there are remaining atoms to be visited and
	 * <code>false</code> otherwise.</p>
	 */
	boolean hasNext();
	
	/**
	 * <p>Return a pair consisting of the link pointing to the next atom in the 
	 * traversal as well as the atom itself. That is, return a 
	 * Pair&lt;handle to link, handle to target atom&gt;.</p>
	 */
	Pair<HGHandle, HGHandle> next();
	
	/**
	 * <p>Return <code>true</code> if the given atom was already visited and 
	 * <code>false</code> otherwise.</p>
	 * 
	 * <p>
	 * An atom is considered visited as soon as it is returned by a call to the 
	 * <code>next</code> method, and not before.
	 * </p>
	 * 
	 * @param handle The handle of the atom.
	 */
	boolean isVisited(HGHandle handle);
}
