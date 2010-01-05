/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

/**
 * <p>
 * A simple interface to be implemented by objects that set values in specific
 * dynamic contexts. This is usual implemented by anonymous classes.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface ValueSetter<T>
{
	void set(T value);
}
