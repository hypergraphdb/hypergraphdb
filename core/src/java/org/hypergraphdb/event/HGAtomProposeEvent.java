/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2018 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * Event triggered when a new atom is about to be added to the graph. Listeners
 * may perform some other operation before the atom is added or may throw an
 * exception to prevent the atom from being added. Returning <code>HGListener.Result.cancel</code>
 * from the listener will also stop the atom addition, but without communicating further information
 * upstream to the application.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGAtomProposeEvent extends HGEventBase
{
    private Object atom;
    private HGHandle type;
    private int flags;
    
    public HGAtomProposeEvent(Object atom, HGHandle type, int flags)
    {
        this.atom = atom;
        this.type = type;
        this.flags = flags;
    }

    public Object getAtom()
    {
        return atom;
    }

    public void setAtom(Object atom)
    {
        this.atom = atom;
    }

    public HGHandle getType()
    {
        return type;
    }

    public void setType(HGHandle type)
    {
        this.type = type;
    }

    public int getFlags()
    {
        return flags;
    }

    public void setFlags(int flags)
    {
        this.flags = flags;
    }    
}