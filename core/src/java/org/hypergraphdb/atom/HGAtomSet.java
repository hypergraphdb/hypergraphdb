/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.HGSortedSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * <p>
 * A <code>HGAtomSet</code> represents a temporary construction of a set of atoms.
 * Each atom handle is stored in the set. The implementation provides:
 * 
 * <ul>
 * <li>Very fast membership operation.</li>
 * <li>Fast addition and removal of elements.</li>
 * <li>Long term persistence of the set. </li>
 * </ul>
 * </p>
 *  
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class HGAtomSet implements HGSortedSet<HGHandle>
{	
    static final long serialVersionUID = -1L;    
	protected HGSortedSet<HGPersistentHandle> impl = null;

	public HGAtomSet()
	{
		this.impl = new ArrayBasedSet<HGPersistentHandle>(new HGPersistentHandle[0]); 
//		this.impl = new LLRBTree<HGPersistentHandle>();
	}
	
	public HGAtomSet(HGSortedSet implementation)
	{
		this.impl = implementation;
	}
	
	public HGRandomAccessResult<HGHandle>  getSearchResult()
	{
		return (HGRandomAccessResult)impl.getSearchResult();
	}
    public Comparator<? super HGHandle> comparator()
	{
		return null;
	}
	public HGHandle first()
	{
		return impl.first();
	}
	public SortedSet<HGHandle> headSet(HGHandle h)
	{
		return (SortedSet)impl.headSet(U.persistentHandle(h));
	}
	public HGHandle last()
	{
		return impl.last();
	}
	public SortedSet<HGHandle> subSet(HGHandle fromElement, HGHandle toElement)
	{
		return (SortedSet)impl.subSet(U.persistentHandle(fromElement), U.persistentHandle(toElement));
	}
	public SortedSet<HGHandle> tailSet(HGHandle h)
	{
		return (SortedSet)impl.tailSet(U.persistentHandle(h));
	}
	public boolean add(HGHandle h)
	{
		return impl.add(U.persistentHandle(h));
	}
	public boolean addAll(Collection<? extends HGHandle> c)
	{
		boolean changed = false;
		for (HGHandle h : c)
			changed = changed || add(h);
		return changed;
	}
	public void clear()
	{
		impl.clear();
	}
	public boolean contains(Object o)
	{
		return impl.contains(U.persistentHandle((HGHandle)o));
	}
	public boolean containsAll(Collection<?> c)
	{
		for (Object o : c)
			if (!contains(o))
				return false;
		return true;
	}
	public boolean isEmpty()
	{
		return impl.isEmpty();
	}
	public Iterator<HGHandle> iterator()
	{
		return (Iterator)impl.iterator();
	}
	public boolean remove(Object o)
	{
		return impl.remove(U.persistentHandle((HGHandle)o));
	}
	public boolean removeAll(Collection<?> c)
	{
		boolean changed = false;
		for (Object o : c)
			changed = changed || remove(o);
		return changed;
	}
	public boolean retainAll(Collection<?> c)
	{
		return impl.retainAll(c);
	}
	public int size()
	{
		return impl.size();
	}
	public Object[] toArray()
	{
		return impl.toArray();
	}
	public <T> T[] toArray(T[] a)
	{
		return impl.toArray(a);
	}
	
	public int hashCode() { return System.identityHashCode(this); }
    public boolean equals(Object x) { return x == this; }
}
