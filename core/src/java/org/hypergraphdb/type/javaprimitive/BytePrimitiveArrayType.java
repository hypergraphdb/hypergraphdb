/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

public class BytePrimitiveArrayType extends PrimitiveArrayType 
{

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for byte array, handle=" + handle.toString());
		byte [] result = new byte[data.length - 1];
		System.arraycopy(data, 1, result, 0, result.length);
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		byte [] A = (byte[])instance;
		byte [] data = new byte[A.length + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);
		System.arraycopy(A, 0, data, 1, A.length);
		return hg.getStore().store(data);
	}
}
