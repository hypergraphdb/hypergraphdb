package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * This condition is satisfied by any and all atoms in the HyperGraph database.
 * That is, it is satisfied given <code>HGHandle</code> if and only if it is
 * an atom in the graph.
 * </p>
 *  
 * <p>
 * When translated to a query alone, it will result in an enumeration of all
 * atoms in the database. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AnyAtomCondition implements HGQueryCondition, HGAtomPredicate 
{

	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		return hg.get(handle) != null;
	}
}
