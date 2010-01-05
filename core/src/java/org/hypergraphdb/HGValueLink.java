/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;


/**
 * <p>
 * <code>HGValueLink</code> is a <code>HGLink</code> that can hold an arbitrary
 * object as payload. The object can be of any type and can be interpreted by the 
 * application, for instance, as a label where it will be usually a <code>String</code>
 * or a weight, if it is a number. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class HGValueLink extends HGPlainLink
{
    private Object value;
    
    public HGValueLink()
    {        
    }
    
    public HGValueLink(HGHandle...targets)
    {
        super(targets);
    }
    
    public HGValueLink(Object value, HGHandle...targets)
    {
        super(targets);
        this.value = value;
    }
    
    public void setValue(Object value)
    {
        this.value = value;
    }
    
    public Object getValue()
    {
        return value;
    }
}
