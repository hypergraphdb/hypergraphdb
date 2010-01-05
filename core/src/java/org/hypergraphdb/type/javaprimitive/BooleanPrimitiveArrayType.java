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

/**
 * <p>
 * Boolean arrays are stored using a byte for each boolean value (as usual, 0 for false and
 * 1 for true). There is aways one byte in front of the actual array to indicate 
 * whether we have an empty array or not. If the array is empty, the first and only
 * byte has a value of 0; otherwise, its value is one.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class BooleanPrimitiveArrayType extends PrimitiveArrayType 
{
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		byte [] data = hg.getStore().getData(handle);
		if (data == null)
			throw new HGException("Could not find value for boolean array, handle=" + handle.toString());
		boolean [] result = new boolean[data.length - 1];
		for (int i = 0; i < result.length; i++)
			result[i] = (data[i + 1] == 1);
		return result;
	}

	public HGPersistentHandle store(Object instance) 
	{
		boolean [] A = (boolean[])instance;		
		byte [] data = new byte[A.length + 1];
		data[0] = (byte)(A.length == 0 ? 0 : 1);
		for (int i = 0; i < A.length; i++)
			data[i + 1] = A[i] ? (byte)1 : (byte)0;
		return hg.getStore().store(data);
	}
}
