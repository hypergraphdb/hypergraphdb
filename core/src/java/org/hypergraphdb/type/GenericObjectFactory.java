/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.lang.reflect.Constructor;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;

/**
 * <p>
 * An <code>ObjectFactory</code> implementation that simply uses a specific <code>Class</code> to 
 * fabricate instances. If the <code>Class</code> has a constructor taking a single argument
 * of type <code>HGHandle[]</code> than this constructor is used in a call to the 
 * <code>make(HGHandle [] targetSet)</code> method. Otherwise, if that latter version of <Code>make</code>
 * is called, an exception will be thrown. Note that the implementation doesn't check whether the
 * <code>Class</code> that it handles actually implements the <code>HGLink</code> interface.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class GenericObjectFactory<T> implements ObjectFactory<T> 
{
	private Class<T> type;
    private Constructor<T> linkConstructor = null;
    
    public GenericObjectFactory(Class<T> type)
    {
    	this.type = type;
        try
        {
        	linkConstructor = type.getDeclaredConstructor(new Class[] {HGHandle[].class} );
        }
        catch (NoSuchMethodException ex) { }
    	
    }
    
	public Class<T> getType() 
	{
		return type;
	}

	public T make() 
	{
		try
		{
			return (T)type.newInstance();
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
	}

	public T make(HGHandle[] targetSet) 
	{
		if (linkConstructor == null)
			throw new HGException("Unable to construct a link of type " + 
					type.getName() + ", the class doesn't have a HGHandle [] based constructor.");
		try
		{
			return (T)linkConstructor.newInstance(new Object[] { targetSet });
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
	}
}
