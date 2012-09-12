/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * 
 * <p>
 * A <code>LinkIndexer</code> indexes atoms by their target <b>ordered</b> set.
 * That is, all targets, in order, are taken to form the key of the index.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class LinkIndexer extends HGKeyIndexer<byte[]>
{
    public LinkIndexer()
    {
    }

    public LinkIndexer(String name, HGHandle type)
    {
        super(name, type);
    }

    public LinkIndexer(HGHandle type)
    {
        super(type);
    }

    public boolean equals(Object other)
    {
        if (other == this)
            return true;
        if (!(other instanceof LinkIndexer))
            return false;
        LinkIndexer idx = (LinkIndexer) other;
        return getType().equals(idx.getType());
    }

    public int hashCode()
    {
        return getType().hashCode();
    }

    public Comparator<byte[]> getComparator(HyperGraph graph)
    {
        return null; // use default byte-by-byte comparator
    }

    public ByteArrayConverter<byte[]> getConverter(HyperGraph graph)
    {
        return BAtoBA.getInstance();
    }

    public byte[] getKey(HyperGraph graph, Object atom)
    {
    	HGLink link = (HGLink) atom;
        
    	if (link.getArity() == 0) {
    		return new byte[0];
    	}
    	else {
    		int handleSize = graph.getPersistentHandle(link.getTargetAt(0)).toByteArray().length;
    		byte[] result = new byte[handleSize * link.getArity()];
    		
    		for (int i = 0; i < link.getArity(); i++)
    		{
    			byte[] src = graph.getPersistentHandle(link.getTargetAt(i)).toByteArray();
    			System.arraycopy(src, 0, result, i * handleSize, handleSize);
    		}
    		return result;
    	}
    }
}
