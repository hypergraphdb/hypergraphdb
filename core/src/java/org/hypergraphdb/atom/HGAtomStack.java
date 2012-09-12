/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;

public class HGAtomStack 
{
	U.HandleLink top = null;
	int size = 0;
	
	public boolean isEmpty()
	{
		return top == null;
	}
	
	public HGHandle peek()
	{
		return top.handle;
	}
	
	public HGHandle pop()
	{
		size--;
		HGHandle result = top.handle;
		top = top.next;
		return result;
	}
	
	public void push(HGHandle handle)
	{
		size++;
		top = new U.HandleLink(handle, top);
	}
}
