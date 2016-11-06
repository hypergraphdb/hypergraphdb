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
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * An indexer that not only determines the key in an index entry, but the value
 * as well. By default, the <code>HGKeyIndexer</code> implementation  provides a key by
 * which to index HyperGraph atoms. In other words, atoms are the "default" values
 * for index entries. A <code>HGValueIndexer</code> provides also the value in an
 * index entry in cases where it is not the atom itself. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public abstract class HGValueIndexer<KeyType, ValueType> implements HGIndexer<KeyType, ValueType>
{
  	private String name = null;
    private HGHandle type;
    	
	public HGValueIndexer()
	{		
	}
	
	public HGValueIndexer(String name, HGHandle type)
	{
		this.name = name;
		this.type = type;
	}
	
	public HGValueIndexer(HGHandle type)
	{
		this.type = type;
	}
	
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public HGHandle getType()
    {
        return type;
    }

    public void setType(HGHandle type)
    {
        this.type = type;
    }
    
    public void index(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, ValueType> index)
    {
        index.addEntry(getKey(graph, atom), getValue(graph, atom));        
    }
    
    public void unindex(HyperGraph graph, HGHandle atomHandle, Object atom, HGIndex<KeyType, ValueType> index)
    {
        index.removeEntry(getKey(graph, atom), getValue(graph, atom));        
    }
    
	/**
	 * <p>
	 * Return the value of an index entry based on the passed in atom. 
	 * </p>
	 */
	public abstract ValueType getValue(HyperGraph graph, Object atom);
	
    /**
     * <p>
     * Returns an index key for the given atom.
     * </p>
     * 
     * @param graph The current <code>HyperGraph</code> instance.
     * @param atom The atom being indexed.
     * @return The index key. If the return value is not a <code>byte[]</code>, 
     * a non-null <code>ByteArrayConverter</code> must be provided by the
     * <code>getConverter</code> method.
     */
    public abstract KeyType getKey(HyperGraph graph, Object atom);
    
	/**
	 * <p>
	 * Return a <code>ByteArrayConverter</code> capable of converting index
	 * entry values to/from byte arrays.
	 * </p>
	 * 
	 * @param graph
	 * @return
	 */
	public abstract ByteArrayConverter<ValueType> getValueConverter(HyperGraph graph);
}