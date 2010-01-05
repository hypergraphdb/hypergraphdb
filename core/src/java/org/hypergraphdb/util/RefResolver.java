/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

/**
 * 
 * <p>
 * Defines a generic capability to resolve a reference of type <code>Key</code> to an
 * object of type <code>Value</code>. This is similar to a {@link LazyRef} but
 * with a key.
 * </p>
 *
 * @author Borislav Iordanov
 */
public interface RefResolver<Key, Value>
{
	Value resolve(Key key);
}
