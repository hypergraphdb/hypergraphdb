/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.LLRBTree;

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
public class HGAtomSet implements SortedSet<HGHandle>
{	
    static final long serialVersionUID = -1L;    
	private LLRBTree<HGPersistentHandle> tree = new LLRBTree<HGPersistentHandle>();

	public HGRandomAccessResult<HGHandle>  getSearchResult()
	{
		return (HGRandomAccessResult)tree.getSearchResult();
	}
    public Comparator<? super HGHandle> comparator()
	{
		return null;
	}
	public HGHandle first()
	{
		return tree.first();
	}
	public SortedSet<HGHandle> headSet(HGHandle h)
	{
		return (SortedSet)tree.headSet(U.persistentHandle(h));
	}
	public HGHandle last()
	{
		return tree.last();
	}
	public SortedSet<HGHandle> subSet(HGHandle fromElement, HGHandle toElement)
	{
		return (SortedSet)tree.subSet(U.persistentHandle(fromElement), U.persistentHandle(toElement));
	}
	public SortedSet<HGHandle> tailSet(HGHandle h)
	{
		return (SortedSet)tree.tailSet(U.persistentHandle(h));
	}
	public boolean add(HGHandle h)
	{
		return tree.add(U.persistentHandle(h));
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
		tree.clear();
	}
	public boolean contains(Object o)
	{
		return tree.contains(U.persistentHandle((HGHandle)o));
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
		return tree.isEmpty();
	}
	public Iterator<HGHandle> iterator()
	{
		return (Iterator)tree.iterator();
	}
	public boolean remove(Object o)
	{
		return tree.remove(U.persistentHandle((HGHandle)o));
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
		return tree.retainAll(c);
	}
	public int size()
	{
		return tree.size();
	}
	public Object[] toArray()
	{
		return tree.toArray();
	}
	public <T> T[] toArray(T[] a)
	{
		return tree.toArray(a);
	}
	
	public int hashCode() { return System.identityHashCode(this); }
    public boolean equals(Object x) { return x == this; }
}