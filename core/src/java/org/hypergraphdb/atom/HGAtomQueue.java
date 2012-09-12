/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;


public class HGAtomQueue 
{	
	U.HandleLink front = null;
	U.HandleLink tail = null;
	int size;
	
	public boolean isEmpty()
	{
		return front == null;
	}
	
	public HGHandle peek()
	{
		return front.handle;
	}
	
	public void enqueue(HGHandle handle)
	{
		size++;
		if (tail == null)
			front = tail = new U.HandleLink(handle, null);
		else
		{
			tail.next = new U.HandleLink(handle, null);
			tail = tail.next;
		}
	}
	
	public HGHandle dequeue()
	{
		size--;
		HGHandle result = front.handle;
		if ( (front = front.next) == null)
			tail = null;
		return result;
	}	
}
