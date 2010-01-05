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

public class LongPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for long array, handle=" + handle.toString());
		long [] result = new long[(data.length - 1)/ 8];
		for (int i = 0; i < result.length; i++)
		{
			int l = 8 * i + 1;
			long lv = ((long)data[l] << 56) +
            		  ((long)(data[l+1] & 255) << 48) +
            		  ((long)(data[l+2] & 255) << 40) +
            		  ((long)(data[l+3] & 255) << 32) +
            		  ((long)(data[l+4] & 255) << 24) +
            		  ((data[l+5] & 255) << 16) + 
            		  ((data[l+6] & 255) <<  8) + 
            		  ((data[l+7] & 255) <<  0);
			result[i] = lv;			
		}
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		long [] A = (long[])instance;
		byte [] data = new byte[A.length * 8 + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);		
		for (int i = 0; i < A.length; i++)
		{
	        long v = A[i];
	        int j = 8 * i + 1;
	        data[j + 0] = (byte) ((v >>> 56));
	        data[j + 1] = (byte) ((v >>> 48));
	        data[j + 2] = (byte) ((v >>> 40));
	        data[j + 3] = (byte) ((v >>> 32));
	        data[j + 4] = (byte) ((v >>> 24));
	        data[j + 5] = (byte) ((v >>> 16));
	        data[j + 6] = (byte) ((v >>> 8));
	        data[j + 7] = (byte) ((v >>> 0));
		}
		return hg.getStore().store(data);
	}
}
