/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

public class IntPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for int array, handle=" + handle.toString());
		int [] A = new int[(data.length - 1)/ 4];
		for (int i = 0; i < A.length; i++)
		{
			int offset = i*4 + 1;
	        int ch1 = data[offset];
	        int ch2 = data[offset + 1];
	        int ch3 = data[offset + 2];
	        int ch4 = data[offset + 3];
	        A[i] = ((ch1 & 0xFF) << 24) | ((ch2 & 0xFF) << 16) | ((ch3 & 0xFF) << 8) | (ch4 & 0xFF);						
		}
		return A;
	}

	public HGPersistentHandle store(Object instance) 
	{
		int [] A = (int []) instance;		
		byte [] data = new byte[4 * A.length + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);		
		for (int i = 0; i < A.length; i++)
		{
			int offset = i*4 + 1;
	        int v = A[i];
	        data[offset + 0] = (byte) ((v >>> 24) & 0xFF); 
	        data[offset + 1] = (byte) ((v >>> 16) & 0xFF);
	        data[offset + 2] = (byte) ((v >>> 8) & 0xFF); 
	        data[offset + 3] = (byte) ((v >>> 0) & 0xFF);
		}
		return hg.getStore().store(data);
	}
}
