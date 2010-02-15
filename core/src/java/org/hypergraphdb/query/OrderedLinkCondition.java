/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A <code>OrderedLinkCondition</code> constraints the query result set to ordered links
 * pointing to a target set of atoms. The target set is specified when
 * the condition is constructed through an array of <code>HGHandle</code>s. This array
 * must have the targets in the desired order. Note that the <code>targetSet</code> parameter
 * of this condition need only be a subset of the examined links' target sets. Thus a link
 * with an ordered target set of (a,b,c,d) would be satisfied by an <code>OrderedLinkCondition</code>
 * constructed with the target set (b,d) for example. If same arity is desired, the <code>ArityCondition</code>
 * should be used in conjunction with this one.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class OrderedLinkCondition implements HGQueryCondition, HGAtomPredicate 
{
	private HGHandle [] targetSet = HyperGraph.EMTPY_HANDLE_SET;
	private byte [] targetsBuffer = null;
	
	private byte [] getTargetsBuffer(HyperGraph graph)
	{
		if (targetsBuffer == null)
		{
			targetsBuffer = new byte[16*targetSet.length];
			for (int i = 0; i < targetSet.length; i++)
			{
				byte [] src = graph.getPersistentHandle(targetSet[i]).toByteArray();
				System.arraycopy(src, 0, targetsBuffer, i*16, 16);
			}
		}
		return targetsBuffer;
	}
	
	public OrderedLinkCondition()
	{
		
	}
	
	public OrderedLinkCondition(HGHandle [] targetSet)
	{
		if (targetSet == null)
			throw new HGException("OrderedLinkCondition instantiated with a null target set.");
        this.targetSet = new HGHandle[targetSet.length];		
        System.arraycopy(targetSet, 0, this.targetSet, 0, targetSet.length);
	}

    public OrderedLinkCondition(List<HGHandle> targetSet)
    {
        if (targetSet == null)
            throw new HGException("OrderedLinkCondition instantiated with a null target set.");
        this.targetSet = new HGHandle[this.targetSet.length];
        int i = 0;
        for (HGHandle h : targetSet)
            this.targetSet[i++] = h; 
    }
	
	public HGHandle [] targets()
	{
		return targetSet;
	}
	
	public HGHandle[] getTargets()
	{
		return targetSet;
	}

	public void setTargets(HGHandle[] targetSet)
	{
		this.targetSet = targetSet;
	}

	public void setTarget(int pos, HGHandle newTarget)
	{
		targetSet[pos] = newTarget;
		byte [] B;
		if (newTarget instanceof HGPersistentHandle)
			B = ((HGPersistentHandle)newTarget).toByteArray();
		else
			B = ((HGLiveHandle)newTarget).getPersistentHandle().toByteArray();
		if (targetsBuffer != null)
			System.arraycopy(B, 0, targetsBuffer, B.length*pos, B.length);
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		// If the atom corresponding to 'handle' is already in the cache, there
		// is no point fetching it from permanent storage. Otherwise, there's no point
		// caching the actual atom...
		if (hg.isLoaded(handle))
		{
			Object atom = hg.get(handle);
			if (! (atom instanceof HGLink))
				return false;
			HGLink link = (HGLink)atom;
			int i = 0, j = 0;
			while (i < link.getArity() && j < targetSet.length)
			{
				if (targetSet[j].equals(link.getTargetAt(i)))
					j++;
				i++;
			}
			return j == targetSet.length;
		}
		else
		{
			byte [] A = hg.getStore().getLinkData(hg.getPersistentHandle(handle));
			byte [] B = getTargetsBuffer(hg);
			byte [] anyBuffer = HGHandleFactory.anyHandle.toByteArray();
			int i = 32, j = 0;
			while (i < A.length && j < B.length)
			{
				if (BAUtils.eq(A, i, B, j, 16) || BAUtils.eq(B, j, anyBuffer, 0, 16))
					j += 16;
				i += 16;
			}
			return j == B.length;
			
/* 			HGPersistentHandle [] A = hg.getStore().getLink(hg.getPersistentHandle(handle));			
			int i = 2, j = 0;
			while (i < A.length && j < targetSet.length)
			{
				if (targetSet[j].equals(A[i]) || targetSet[j].equals(HGHandleFactory.anyHandle))
					j++;
				i++;
			}
			return j == targetSet.length; */ 			
		}
	}

	public String toString()
	{
		StringBuffer result = new StringBuffer("orderedLinks(");
		for (int i = 0; i < targetSet.length; i++)
		{
			result.append(targetSet[i]);
			if (i < targetSet.length - 1)
				result.append(",");
		}
		result.append(")");
		return result.toString();
	}
	
	public int hashCode() 
	{ 
		int x = 0;
		for (HGHandle h : targetSet) x += h.hashCode();
		return x;
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof OrderedLinkCondition))
			return false;
		else
			return HGUtils.eq(targetSet, ((OrderedLinkCondition)x).targetSet);
	}	
}
