package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;

/**
 * 
 * <p>
 * The breadth-first search variant of a {@link TraversalCondition}.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class BFSCondition extends TraversalCondition
{

	public BFSCondition(HGHandle startAtom)
	{
		super(startAtom);
	}
	
	@Override
	public HGTraversal getTraversal(HyperGraph graph)
	{
		return new HGBreadthFirstTraversal(getStartAtom(), makeGenerator(graph));
	}
}
