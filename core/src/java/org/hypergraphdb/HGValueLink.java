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
 * <code>HGValueLink</code> is a {@link HGLink} that can hold an arbitrary
 * object as payload. The object can be of any type and can be interpreted by the 
 * application, for instance, as a label where it will be usually a <code>String</code>
 * or a weight, if it is a number. Note that the type of the stored atom will be
 * the type of wrapped object, not <code>HGValueLink.class</code>. Thus, if you are wrapping
 * a Java String as a link, for example, you would query with <code>hg.type(String.class)</code>
 * rather than <code>hg.type(HGValueLink.class)</code>.
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
    
    /**
     * Set the underlying value.
     */
    public void setValue(Object value)
    {
        this.value = value;
    }

    /**
     * Return the underlying value.
     */
    public Object getValue()
    {
        return value;
    }
    
    public String toString()
    {
    	StringBuilder b = new StringBuilder(value == null ? "null" : value.toString());
    	b.append('[');
    	for (int i = 0; i < getArity(); i++)
    	{
    		b.append(outgoingSet[i]);
    		if (i < getArity() - 1)
    		    b.append(",");
    	}
    	b.append(']');
    	return b.toString();
    }
}