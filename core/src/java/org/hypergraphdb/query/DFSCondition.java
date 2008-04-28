package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.HGDepthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;

/**
 * 
 * <p>
 * The depth-first search variant of a {@link TraversalCondition}.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DFSCondition extends TraversalCondition
{
	public DFSCondition(HGHandle startAtom)
	{
		super(startAtom);
	}
	
	@Override
	public HGTraversal getTraversal(HyperGraph graph)
	{
		return new HGDepthFirstTraversal(getStartAtom(), makeGenerator(graph));
	}
}