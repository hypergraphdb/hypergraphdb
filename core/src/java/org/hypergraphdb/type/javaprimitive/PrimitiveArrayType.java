/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGAtomType;

public abstract class PrimitiveArrayType implements HGAtomType 
{
	protected HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public void release(HGPersistentHandle handle) 
	{
		hg.getStore().removeData(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		// TODO
		return false;
	}
}
