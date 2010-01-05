/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A <code>Slot</code> represents a placeholder in a record type. A slot has a label, which
 * is simply a string a type handle that constraints the
 * type of the value it can hold. Note that this class does not hold actual slot <em>values</em>. 
 * Rather, it is a descriptor of a record slot instances. A <code>RecordType</code> is defined
 * simply as a set of <code>Slot</code>s. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class Slot
{
    private String label;
    private HGHandle valueType;
    
    public Slot()
    {        
    }
    
    public Slot(String label, HGHandle valueType)
    {
        this.label = label;
        this.valueType = valueType;
    }

    /**
     * @return Returns the label.
     */
    public String getLabel()
    {
        return label;
    }

    /**
     * @param label The label to set.
     */
    public void setLabel(String label)
    {
        this.label = label;
    }

    /**
     * @return Returns the valueType.
     */
    public HGHandle getValueType()
    {
        return valueType;
    }

    /**
     * @param valueType The valueType to set.
     */
    public void setValueType(HGHandle valueType)
    {
        this.valueType = valueType;
    }
        
    public int hashCode()
    {
        return HGUtils.hashThem(label, valueType);
    }
        
    public boolean equals(Object other)
    {
        if (! (other instanceof Slot))
            return false;
        else
        {
            Slot otherSlot = (Slot)other;
            return HGUtils.eq(label, otherSlot.label) && HGUtils.eq(valueType, otherSlot.valueType);
        }
    }
    
    public String toString()
    {
    	return "slot(" + label + "," + valueType + ")";
    }
}
