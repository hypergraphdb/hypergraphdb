/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.util.HGSortedSet;

/**
 * 
 * <p>
 * Represents an atom incidence set. That is, a set containing all atoms pointing to a given
 * atom. Instances of this class can be cached and queried in memory. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public final class IncidenceSet extends HGAtomSet
{
	private HGHandle atom;
	
	public IncidenceSet(HGHandle atom, HGSortedSet<HGHandle> impl)
	{
		super(impl);
		this.atom = atom;
	}
	
	/**
	 * <p>Return the atom whose incidence set this instance represents.
	 */
	public HGHandle getAtom()
	{
		return atom;
	}
}