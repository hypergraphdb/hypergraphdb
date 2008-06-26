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
public final class IncidenceSet
{
	private HGHandle atom;
	private HGAtomSet set;
	
	public IncidenceSet(HGHandle atom)
	{
		this.atom = atom;
	}
	
	public HGHandle getAtom()
	{
		return atom;
	}
	
	public int size()
	{
		return set.size();
	}
	
	/**
	 * <p>Return a search result object for querying this incidence set. Note that even
	 * though an <code>IncidenceSet</code> is usually an in-memory only representation, 
	 * the return result set should be closed like all other result sets.</p>
	 */
	public HGRandomAccessResult getSearchResult()
	{
		return null;
	}
}