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
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * A {@link HGValueIndexer} for HyperGraph links where the key in the index is
 * one of the targets within a link and the value is another one of the targets.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class TargetToTargetIndexer extends HGValueIndexer<HGHandle, HGHandle>
{
    private int fromTarget, toTarget;

    public TargetToTargetIndexer()
    {
    }

    public TargetToTargetIndexer(HGHandle type, int fromTarget, int toTarget)
    {
        super(type);
        this.fromTarget = fromTarget;
        this.toTarget = toTarget;
    }

    public TargetToTargetIndexer(String name, HGHandle type, int fromTarget, int toTarget)
    {
        super(name, type);
        this.fromTarget = fromTarget;
        this.toTarget = toTarget;
    }

    public int getFromTarget()
    {
        return fromTarget;
    }

    public void setFromTarget(int fromTarget)
    {
        this.fromTarget = fromTarget;
    }

    public int getToTarget()
    {
        return toTarget;
    }

    public void setToTarget(int toTarget)
    {
        this.toTarget = toTarget;
    }

    public HGHandle getKey(HyperGraph graph, Object atom)
    {
        return ((HGLink) atom).getTargetAt(fromTarget);
    }

    public HGHandle getValue(HyperGraph graph, Object atom)
    {
        return ((HGLink) atom).getTargetAt(toTarget);
    }

    public ByteArrayConverter<HGHandle> getValueConverter(final HyperGraph graph)
    {
        return new ByteArrayConverter<HGHandle>()
        {
            public byte[] toByteArray(HGHandle h)
            {
                return graph.getPersistentHandle(h).toByteArray();
            }

            public HGHandle fromByteArray(byte[] A, int offset, int length)
            {
                return graph.getHandleFactory().makeHandle(A, offset);
            }
        };
    }

    public Comparator<byte[]> getComparator(HyperGraph graph)
    {
        return null;
    }

    public ByteArrayConverter<HGHandle> getConverter(final HyperGraph graph)
    {
        return new ByteArrayConverter<HGHandle>()
        {
            public byte[] toByteArray(HGHandle h)
            {
                return graph.getPersistentHandle(h).toByteArray();
            }

            public HGHandle fromByteArray(byte[] A, int offset, int length)
            {
                return graph.getHandleFactory().makeHandle(A, offset);
            }
        };
    }

    public int hashCode()
    {
        return HGUtils.hashThem(getType(), HGUtils.hashThem(fromTarget,
                toTarget));
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof TargetToTargetIndexer))
            return false;
        TargetToTargetIndexer i = (TargetToTargetIndexer) other;
        return HGUtils.eq(getType(), i.getType()) && fromTarget == i.fromTarget
                && toTarget == i.toTarget;
    }
}
