/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;

/**
 *
 * @author  User
 */
public abstract class NumericTypeBase extends PrimitiveTypeBase
{
	private static final NumericComparator comp = new NumericComparator();
	
    public static class NumericComparator implements Comparator<byte[]>
    {
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
        return comp;
    }
}