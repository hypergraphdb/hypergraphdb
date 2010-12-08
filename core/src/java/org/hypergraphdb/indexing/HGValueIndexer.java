/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.indexing;

import org.hypergraphdb.HGHandle;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * An indexer that not only determines the key in an index entry, but the value
 * as well. By default, <code>HGIndexer</code> implementation  provide a key by
 * which to index hypergraph atoms. In other words, atoms are the "default" values
 * for index entries. A <code>HGValueIndexer</code> provides also the value in an
 * index entry in cases where it is not the atom itself. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public abstract class HGValueIndexer extends HGKeyIndexer
{
	public HGValueIndexer()
	{		
	}
	
	public HGValueIndexer(HGHandle type)
	{
		super(type);
	}
	
    @SuppressWarnings("unchecked")
    public void index(HyperGraph graph, HGPersistentHandle atomHandle, Object atom, HGIndex index)
    {
        index.addEntry(getKey(graph, atom), getValue(graph, atom));        
    }

    
    @SuppressWarnings("unchecked")
    public void unindex(HyperGraph graph, HGPersistentHandle atomHandle, Object atom, HGIndex index)
    {
        index.removeEntry(getKey(graph, atom), getValue(graph, atom));        
    }
    
	/**
	 * <p>
	 * Return the value of an index entry based on the passed in atom. 
	 * </p>
	 */
	public abstract Object getValue(HyperGraph graph, Object atom);
	
	/**
	 * <p>
	 * Return a <code>ByteArrayConverter</code> capable of converting index
	 * entry values to/from byte arrays.
	 * </p>
	 * 
	 * @param graph
	 * @return
	 */
	public abstract ByteArrayConverter<?> getValueConverter(HyperGraph graph);
}