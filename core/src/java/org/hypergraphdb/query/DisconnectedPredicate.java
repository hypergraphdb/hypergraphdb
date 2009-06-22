package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * A predicate that returns <code>true</code> if the incidence set of a given atom
 * is empty and <code>false</code> otherwise.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DisconnectedPredicate implements HGAtomPredicate, HGQueryCondition
{
    public boolean satisfies(HyperGraph graph, HGHandle handle)
    {
        return graph.getIncidenceSet(handle).isEmpty();
    }
}