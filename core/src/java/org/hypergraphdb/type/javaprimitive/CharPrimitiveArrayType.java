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

public class CharPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for char array, handle=" + handle.toString());
		char [] result = new char[(data.length - 1) / 2];
		for (int i = 0; i < result.length; i++)
		{
	        int ch1 = data[2 * i + 1];
	        int ch2 = data[2 * i + 1 + 1];
	        result[i] = (char) ((ch1 << 8) + (ch2 << 0));    			
		}
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		char [] A = (char[])instance;
		byte [] data = new byte[1 + A.length*2];
		data[0] = (byte)(A.length == 0 ? 0 : 1);		
		for (int i = 0; i < A.length; i++)
		{
	        data[1 + 2 * i] = (byte) ((A[i] >>> 8) & 0xFF); 
	        data[1 + 2 * i + 1] = (byte) ((A[i] >>> 0) & 0xFF);			
		}
		return hg.getStore().store(data);
	}
}
