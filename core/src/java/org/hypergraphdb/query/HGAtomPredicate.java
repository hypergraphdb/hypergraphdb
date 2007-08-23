package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * This interface defines a predicate of a single atom. There is
 * no guarantee as to the running time of a predicate, but in general
 * implementation should strive not to do explicit reads from the 
 * data store whenever possible.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface HGAtomPredicate
{
	/**
	 * <p>Check whether a given <code>Object</code> satisfies this
	 * query condition.</p>
     * 
	 * @param hg The <code>HyperGraph</code> instance.
	 * @param handle The atom on which to test the query condition.
	 * 
	 * @return <code>true</code> if the passed in parameter satisfies
	 * the condition and <code>false</code> otherwise.
	 */
	boolean satisfies(HyperGraph hg, HGHandle handle);
}