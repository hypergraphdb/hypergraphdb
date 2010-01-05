/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGQuery;

/**
 * <p>
 * A <code>KeyBasedQuery</code> is a <code>HGQuery</code> that produces
 * a result based on a single key value. A query implementation that
 * supports this interface allows the client to set a different key and
 * execute the query possibly multiple times, in order to obtain a different
 * result.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public abstract class KeyBasedQuery<Key, Value> extends HGQuery<Value> 
{
	/**
	 * <p>Specify the key on which this query operates.</p>
	 * 
	 * @param key The key object. The value may be interpreted differently
	 * based on the concrete <code>HGQuery</code> instance. Usually it
	 * should be either a <code>byte []</code> or convertible to one.
	 */
	public abstract void setKey(Key key);
	
	/**
	 * <p>Retrieve the key object used to perform this query.</p>
	 */
	public abstract Key getKey();
}
