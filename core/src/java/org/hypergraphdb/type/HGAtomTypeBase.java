/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * A base class for implementing <code>HGAtomType</code>. Holds a protected reference
 * to the <code>HyperGraph</code> instance and implements <code>subsumes</code>
 * with <code>org.hypergraphdb.util.HGUtils.eq</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public abstract class HGAtomTypeBase implements HGAtomType
{
	@HGIgnore
	protected HyperGraph graph;
	
	public void setHyperGraph(HyperGraph hg)
	{
		this.graph = hg;
	}

	public HyperGraph getHyperGraph()
	{
		return graph;
	}
	
	public boolean subsumes(Object general, Object specific)
	{
		return HGUtils.eq(general, specific);
	}
}
