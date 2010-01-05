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

public class ShortPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for short array, handle=" + handle.toString());		
		short [] A = new short[(data.length - 1)/ 2];
		for (int i = 0; i < A.length; i++)
		{
			int offset = 2*i + 1;
			A[i] = (short) (((data[offset + 1] & 0xFF) << 0) + ((data[offset]) << 8));					
		}
		return A;
	}

	public HGPersistentHandle store(Object instance) 
	{
		short [] A = (short []) instance;		
		byte [] data = new byte[2 * A.length + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);		
		for (int i = 0; i < A.length; i++)
		{
	        short v = A[i];
	        int offset = 2*i + 1;
	        data[offset + 1] = (byte) (v >>> 0);
	        data[offset] = (byte) (v >>> 8);	        
		}
		return hg.getStore().store(data);
	}
}
