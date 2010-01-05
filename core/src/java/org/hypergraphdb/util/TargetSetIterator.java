/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.TwoWayIterator;

/**
 * 
 * <p>
 * An iterator over the target set of a given link.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class TargetSetIterator implements TwoWayIterator<HGHandle>
{
    private int pos = -1;
    private HGLink link;
    
    public TargetSetIterator(HGLink link)
    {
        this.link = link;
    }
    
    public boolean hasNext()
    {
        return pos + 1 < link.getArity();
    }

    public HGHandle next()
    {
        return link.getTargetAt(++pos);
    }

    public boolean hasPrev()
    {
        return pos > -1;
    }

    public HGHandle prev()
    {
        return link.getTargetAt(pos--);
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
