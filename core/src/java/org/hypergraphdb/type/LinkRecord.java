/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;

/**
 * <p>
 * This class represents a generic implementation of a record that is also a
 * <code>HGLink</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class LinkRecord extends Record implements HGLink
{
    private HGHandle [] targets;
    
    public LinkRecord(HGHandle type)
    {        
        super(type);
        targets = new HGHandle[0];
    }
    
    public LinkRecord(HGHandle type, HGHandle [] targets)
    {
        super(type);
        if (targets == null)
            throw new NullPointerException("Attempt to construct a LinkRecord with a null target set.");
        this.targets = targets;
    }
    
    public int getArity()
    {        
        return targets.length;
    }

    public HGHandle getTargetAt(int i)
    {
        return targets[i];
    }

    public void notifyTargetHandleUpdate(int i, HGHandle handle)
    {
        targets[i] = handle;
    }
    
    public void notifyTargetRemoved(int i)
    {
    	HGHandle [] newOutgoing = new HGHandle[targets.length - 1];
    	System.arraycopy(targets, 0, newOutgoing, 0, i);
    	System.arraycopy(targets, i + 1, newOutgoing, i, targets.length - i -1);
    	targets = newOutgoing;
    }    
}
