/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;

/**
 * <p>
 * This class represents a simple, temporary link used during various query and
 * graph traversal activities. It is just a <code>HGLink</code> wrapper around
 * an array of <code>HGHandle</code>s. It is needed by APIs that rely solely on
 * the <code>HGLink</code> interface, but need to work with temporary link
 * representations in the form of <code>HGHandle[]</code>.
 * </p>
 * 
 * <p>
 * Note that the implementation never checks for null when accessing its
 * HGHandle array argument.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class TempLink implements HGLink
{
	private HGHandle[] array;
	private int start, end;

	public TempLink(HGHandle[] array)
	{
		setHandleArray(array);
	}

	public TempLink(HGHandle[] array, int start)
	{
		setHandleArray(array, start);
	}

	public TempLink(HGHandle[] array, int start, int end)
	{
		setHandleArray(array, start, end);
	}

	public void setHandleArray(HGHandle[] array)
	{
		this.array = array;
		start = 0;
		end = array.length;
	}

	public void setHandleArray(HGHandle[] array, int start)
	{
		this.array = array;
		this.start = start;
		end = array.length;
	}

	public void setHandleArray(HGHandle[] array, int start, int end)
	{
		this.array = array;
		this.start = start;
		this.end = end;
	}

	public int getArity()
	{
		return end - start;
	}

	public HGHandle getTargetAt(int i)
	{
		return array[start + i];
	}

	public void notifyTargetHandleUpdate(int i, HGHandle handle)
	{
		array[start + i] = handle;
	}

	public void notifyTargetRemoved(int i)
	{
		HGHandle[] newOutgoing = new HGHandle[array.length - 1];
		System.arraycopy(array, 0, newOutgoing, 0, i);
		System.arraycopy(array, i + 1, newOutgoing, i, array.length - i - 1);
		array = newOutgoing;
	}
}
