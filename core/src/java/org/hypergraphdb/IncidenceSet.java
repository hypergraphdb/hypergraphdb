package org.hypergraphdb;

import org.hypergraphdb.atom.HGAtomSet;

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
	
	public IncidenceSet(HGHandle atom)
	{
		this.atom = atom;
	}
	
	public HGHandle getAtom()
	{
		return atom;
	}
}