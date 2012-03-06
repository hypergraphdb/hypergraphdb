/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.algorithms;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * This interface defines an adjacency list (AL) generator. An AL generator is capable
 * of producing all atoms adjacent to a given atom. Because with HyperGraphDB, a single 
 * link can be the source of several adjacent atoms, and moreover two atoms may be adjacent
 * through more that one link, one needs both the link and the target atom when enumerating
 * the neighborhood of a given atom. The <code>HGAlGenerator</code> interface provides
 * both those pieces of information. All target atoms are streamed in the iterator returned 
 * by the <code>generate</code> method. At each call to <code>next</code> of that iterator,
 * a given link to the atom of interest is being examined. This link's atom handle is 
 * returned by the <code>getCurrentLink</code> method. 
 * </p>
 * 
 * <p>
 * Note that because two atoms may be linked in more than one way, it is possible for
 * the iterator returned by <code>generate</code> to produce the same target atom more
 * than once. Usually however, an <code>HGALGenerator</code> implementation will consider
 * only links of interest in a particular context and such duplication will not occur. 
 * </p>
 * 
 * <p>The order in which the atoms are 
 * produced may or may not be important for the particular algorithm being applied,
 * but a graph traversal takes that order into consideration simply in virtue of its
 * implementation. Hence, an AL generator, when used in combination with a graph traversal,
 * may rely on the fact that atoms will be explored in order.
 * </p>
 * 
 * <p>
 * Note that AL generators return the atoms in the form of a <code>HGSearchResult</code>
 * which may or may not contain references to external resources (such as database
 * cursors). As all <code>HGSearchResult</code> instances, they must be closed in all
 * situation (normal or exceptional) after one is done with them. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGALGenerator 
{
	/**
	 * <p>
	 * Return {@link HGSearchResult}  over all atoms adjacent to the passed in
	 * atom. The result set items are pairs of the form [link, atom] where the first
	 * element is the {@link HGLink} handle of the link that "leads" to the adjacent atom 
	 * and the second element is the adjacent atom itself.   
	 * </p>
	 * 
	 * @param h The handle of the atom of interest.
	 */
	HGSearchResult<Pair<HGHandle, HGHandle>> generate(HGHandle h);	
}