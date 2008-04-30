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
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.atom.impl.UUIDTrie;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

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
 * <p>
 * Normally, an instance of <code>HGAtomSet</code> is added to HyperGraph with
 * the <code>MANAGED</code> system flag. Thus,
 * as with all HyperGraph managed atoms, a <code>HGAtomSet</code> may be removed
 * from permanent storage at the discretion of the system, following the usage
 * frequency of this atom.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class HGAtomSet extends AbstractSet<HGHandle> implements Set<HGHandle>, Cloneable, java.io.Serializable
{	
    static final long serialVersionUID = -1L;
    
	UUIDTrie trie = new UUIDTrie();
	private int size = 0;
	
	public boolean add(HGHandle h)
	{
		if (trie.add(U.getBytes(h)))
		{
			size++;
			return true;
		}
		else 
			return false;
	}
	
	public boolean remove(HGHandle h)
	{
		if (trie.remove(U.getBytes(h)))
		{
			size--;
			return true;
		}
		else
			return false;
	}
	
	public boolean contains(HGHandle h)
	{
		return trie.find(U.getBytes(h));		
	}
	
	public int size()
	{
		return size;
	}
	
	public boolean isEmpty()
	{
		return size == 0;
	}
	
	public void clear()
	{
		trie.clear();
	}
	
    public Object clone() 
    {
    	HGAtomSet newSet = new HGAtomSet();
    	newSet.trie = trie.clone();
    	return newSet;
    }
    
	/**
	 * <p>Return an <code>Iterator</code> over all atoms in this set. Contrary to many
	 * iterator implementations, the <code>remove</code> of the returned iterator is
	 * actually a defined operation and one can use the iterator to selectively remove
	 * elements from the set.</p>
	 */
	public Iterator<HGHandle> iterator()
	{
		final Iterator<byte[]> trieIterator = trie.iterator();
		return new Iterator<HGHandle>()
		{
			public boolean hasNext()
			{
				return trieIterator.hasNext();
			}
			
			public HGHandle next()
			{
				return HGHandleFactory.makeHandle(trieIterator.next());
			}
			
			public void remove()
			{
				trieIterator.remove();
			}
		};
	}
	
    public int hashCode() { return System.identityHashCode(this); }
    public boolean equals(Object x) { return x == this; }
}