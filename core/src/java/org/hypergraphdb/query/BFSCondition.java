/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.util.Ref;

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
	public BFSCondition()
	{
		
	}
	
	public BFSCondition(HGHandle startAtom)
	{
		super(startAtom);
	}
	
	public BFSCondition(Ref<HGHandle> startAtom)
	{
		super(startAtom);
	}
	
	@Override
	public HGTraversal getTraversal(HyperGraph graph)
	{
		return new HGBreadthFirstTraversal(getStartAtomReference(), makeGenerator(graph));
	}
}