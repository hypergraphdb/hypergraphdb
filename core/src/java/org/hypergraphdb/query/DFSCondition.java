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
import org.hypergraphdb.algorithms.HGDepthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.util.Ref;

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
	public DFSCondition()
	{
		
	}
	
	public DFSCondition(HGHandle startAtom)
	{
		super(startAtom);
	}
	
	public DFSCondition(Ref<HGHandle> startAtom)
	{
		super(startAtom);
	}
	
	@Override
	public HGTraversal getTraversal(HyperGraph graph)
	{
		return new HGDepthFirstTraversal(getStartAtomReference(), makeGenerator(graph));
	}
}