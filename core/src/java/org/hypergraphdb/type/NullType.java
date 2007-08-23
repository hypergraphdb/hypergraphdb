/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.LazyRef;

public class NullType implements HGAtomType
{
	public void setHyperGraph(HyperGraph hg)
	{
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet)
	{
		return null;
	}

	public HGPersistentHandle store(Object instance)
	{
		return HGHandleFactory.makeHandle();
	}

	public void release(HGPersistentHandle handle)
	{
	}

	public boolean subsumes(Object general, Object specific)
	{
		return false;
	}
}