/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGSubsumes;

class SubsumesImpl 
{
	protected final HGHandle getTypeFor(HyperGraph hg, HGHandle h)
	{
		if (h == null)
			return null;
		else
			return hg.getType(h);
	}
	
	protected final HGHandle getTypeFor(HyperGraph hg, Object atom)
	{
		if (atom == null)
			return null;
		HGHandle h = hg.getHandle(atom);
		if (h == null)
			return hg.getTypeSystem().getTypeHandle(atom.getClass());
		else
			return hg.getType(h);
	}
	
	protected final boolean declaredSubsumption(HyperGraph hg, HGHandle general, HGHandle specific)
	{
		And subsumesCondition = new And(
		        new AtomTypeCondition(hg.getTypeSystem().getTypeHandle(HGSubsumes.class)),
		        new OrderedLinkCondition(new HGHandle[] { general, specific} )
			); 
			
		HGSearchResult<HGHandle> rs = hg.find(subsumesCondition);
		boolean result = rs.hasNext();
		rs.close();
		return result;
	}
}
