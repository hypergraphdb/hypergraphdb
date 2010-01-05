/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGSubsumes;

class SubsumesImpl 
{
	protected final HGHandle getTypeFor(HyperGraph graph, HGHandle h)
	{
		if (h == null)
			return null;
		else
			return graph.getType(h);
	}
	
	protected final HGHandle getTypeFor(HyperGraph graph, Object atom)
	{
		if (atom == null)
			return null;
		HGHandle h = graph.getHandle(atom);
		if (h == null)
			return graph.getTypeSystem().getTypeHandle(atom.getClass());
		else
			return graph.getType(h);
	}
	
	protected final boolean declaredSubsumption(HyperGraph graph, HGHandle general, HGHandle specific)
	{
		And subsumesCondition = new And(
		        new AtomTypeCondition(graph.getTypeSystem().getTypeHandle(HGSubsumes.class)),
		        new OrderedLinkCondition(new HGHandle[] { general, specific} )
			); 
			
		HGSearchResult<HGHandle> rs = null;
		try
		{
		    rs = graph.find(subsumesCondition);
		    return rs.hasNext();
		}
		finally
		{
		    if (rs != null) rs.close();
		}
	}
}
