/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

public class LinkType implements HGAtomType 
{
	public void setHyperGraph(HyperGraph hg) 
	{
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		throw new HGException("Can't make an instance of abstract type HGLink.");
	}

	public HGPersistentHandle store(Object instance) 
	{
		throw new HGException("Can't store an instance of abstract type HGLink.");		
	}

	public void release(HGPersistentHandle handle) 
	{
		throw new HGException("Can't release an instance of abstract type HGLink.");
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return general.equals(specific);
	}
}
