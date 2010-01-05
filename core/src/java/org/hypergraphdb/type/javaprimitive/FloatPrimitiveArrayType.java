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

public class FloatPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for float array, handle=" + handle.toString());		
		float [] result = new float[(data.length - 1)/ 4];
		for (int i = 0; i < result.length; i++)
		{
	        int j = 4 * i + 1;
	        int fi = ((data[j + 3] & 0xFF) << 0) +
					 ((data[j + 2] & 0xFF) << 8) +
					 ((data[j + 1] & 0xFF) << 16) +
					 ((data[j + 0]) << 24);
		    result[i] = Float.intBitsToFloat(fi);
		}		
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		float [] A = (float [])instance;
		byte [] data = new byte[A.length * 4 + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);		
		for (int i = 0; i < A.length; i++)
		{
	        int fi = Float.floatToIntBits(A[i]);
	        int j = 4 * i + 1;
	        data[j + 3] = (byte) (fi >>> 0);
	        data[j + 2] = (byte) (fi >>> 8);
	        data[j + 1] = (byte) (fi >>> 16);
	        data[j] = (byte) (fi >>> 24);			
		}
		return hg.getStore().store(data);
	}
}
