/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;

public abstract class NumericTypeBase<T extends Number> extends PrimitiveTypeBase<T>
{
	// Note that this comparator is not used anymore as it is actually incorrect.
	// However, I've put it back in the codebase because some older DB instances 
	// won't open without it. It should be eventually removed.
	public static final NumericComparator COMPARATOR = new NumericComparator();
     
	/**
	 * @deprecated
	 */
    public static class NumericComparator implements Comparator<byte[]>, java.io.Serializable
    {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public int compare(byte[] left, byte [] right)
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
     
    public Comparator<byte[]> getComparator()
    {
        return COMPARATOR;
    }	
}