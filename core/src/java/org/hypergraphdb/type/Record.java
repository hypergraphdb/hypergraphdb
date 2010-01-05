/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.HashMap;
import java.util.Iterator;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * The <code>Record</code> implements a generic hypergraph record structure. It is a map between
 * slots and values.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class Record
{
    private HGHandle type;
    private HashMap<Slot, Object> elements = new HashMap<Slot, Object>();
    
    public Record(HGHandle type)
    {        
        this.type = type;
    }
    
    public HGHandle getTypeHandle()
    {
        return type;
    }
    
    public Iterator<Slot> getSlots()
    {
        return elements.keySet().iterator();
    }
    
    public void set(Slot slot, Object value)
    {
        elements.put(slot, value);
    }
    
    public Object get(Slot slot)
    {
        return elements.get(slot);
    }
}
