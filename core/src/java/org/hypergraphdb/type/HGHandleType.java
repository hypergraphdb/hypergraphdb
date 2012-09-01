/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Comparator;

import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.type.javaprimitive.PrimitiveTypeBase;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;


public class HGHandleType extends PrimitiveTypeBase<HGHandle> 
{
	private static final HandleComparator comp = new HandleComparator();
	
    public static class HandleComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;    	
        public int compare(byte [] left, byte [] right)
        {
            int i = dataOffset;
            for (; i < left.length && i < right.length; i++)
                if (left[i] - right[i] == 0)
                    continue;
                else 
                    return left[i] - right[i];
            return 0;
        }
    }

    public static final String INDEX_NAME = "hg_handle_value_index";
    
	@Override
	protected String getIndexName() 
	{
		return INDEX_NAME;
	}

	@Override
	protected HGHandle readBytes(byte[] data, int offset) 
	{
		return graph.getHandleFactory().makeHandle(data, offset);
	}

	@Override
	protected byte[] writeBytes(HGHandle value) 
	{
		if (value instanceof HGPersistentHandle)
			return ((HGPersistentHandle)value).toByteArray();
		else
			return ((HGLiveHandle)value).getPersistent().toByteArray();
	}

	public Comparator<byte[]> getComparator() 
	{
		return comp;
	}	
}
