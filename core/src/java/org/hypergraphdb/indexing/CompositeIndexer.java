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
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

@SuppressWarnings("unchecked")
public class CompositeIndexer extends HGKeyIndexer
{
    private HGKeyIndexer[] indexerParts = null;

    public CompositeIndexer()
    {
    }

    public CompositeIndexer(HGHandle type, HGKeyIndexer[] indexerParts)
    {
        super(type);
        if (indexerParts == null || indexerParts.length == 0)
            throw new IllegalArgumentException(
                    "Attempt to construct CompositeIndexer with null or empty parts.");
        this.indexerParts = indexerParts;
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof CompositeIndexer))
            return false;
        return HGUtils
                .eq(indexerParts, ((CompositeIndexer) other).indexerParts);
    }

    public Comparator<?> getComparator(HyperGraph graph)
    {
        return null;
    }

    public ByteArrayConverter<?> getConverter(HyperGraph graph)
    {
        return BAtoBA.getInstance();
    }

    public Object getKey(HyperGraph graph, Object atom)
    {
        byte[][] keys = new byte[indexerParts.length][];
        int size = 1;
        for (int i = 0; i < indexerParts.length; i++)
        {
            HGKeyIndexer ind = indexerParts[i];
            Object key = ind.getKey(graph, atom);
            keys[i] = ((ByteArrayConverter<Object>) ind.getConverter(graph))
                    .toByteArray(key);
            size += keys[i].length + 4;
        }
        byte[] B = new byte[size];
        B[1] = (byte) keys.length;
        int pos = 1;
        for (byte[] curr : keys)
        {
            BAUtils.writeInt(curr.length, B, pos);
            pos += 4;
            System.arraycopy(curr, 0, B, pos, curr.length);
            pos += curr.length;
        }
        return B;
    }

    public int hashCode()
    {
        if (indexerParts == null)
            return 0;
        int x = indexerParts.length;
        for (HGIndexer ind : indexerParts)
            x ^= ind.hashCode() >> 16;
        return x;
    }

    public HGKeyIndexer[] getIndexerParts()
    {
        return indexerParts;
    }

    public void setIndexerParts(HGKeyIndexer[] indexerParts)
    {
        this.indexerParts = indexerParts;
    }
}
