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
import java.util.Iterator;

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
public final class HGAtomSet 
{	
	UUIDTrie trie = new UUIDTrie();
	
	public boolean add(HGHandle h)
	{
		return trie.add(U.getBytes(h));
	}
	
	public boolean remove(HGHandle h)
	{
		return trie.remove(U.getBytes(h));		
	}
	
	public boolean contains(HGHandle h)
	{
		return trie.find(U.getBytes(h));		
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
}