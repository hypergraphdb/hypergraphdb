/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Mapping;

public final class DerefMapping<T> implements Mapping<HGHandle, T>
{
	private HyperGraph graph;
	
	public DerefMapping(HyperGraph graph)
	{
		this.graph = graph;		 
	}
	
	public T eval(HGHandle x)
	{		
		return graph.get(x);
	}
	
	public HyperGraph getGraph()
	{
		return graph;
	}
}
