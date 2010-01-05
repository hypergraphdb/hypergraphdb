/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * An <code>ObjectFactory</code> is capable of constructing concrete run-time
 * instances of a certain <code>Class</code>. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public interface ObjectFactory<T> 
{
	/**
	 * <p>
	 * Return the <code>Class</code> of the objects being constructed by this factory.
	 * </p> 
	 */
	Class<T> getType();
	
	/**
	 * <p>Create a new run-time instance of the type this factory 
	 * is responsible for.</p>
	 */
	T make();
	
	/**
	 * <p>Create a new run-time <code>HGLink</code> instance of the type this factory is
	 * responsible for.</p>
	 */
	T make(HGHandle [] targetSet);
}
