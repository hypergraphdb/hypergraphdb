/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import java.util.Arrays;
import java.util.Iterator;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;


/**
 * <p>
 * A <code>PipeQuery</code> pipes the output of one query as the 
 * input of another. The piped into query must be a <code>KeyBasedQuery</code>.
 * 
 * </p>
 * @author Borislav Iordanov
 */
public class PipeQuery<Key, Value> extends HGQuery<Value> implements Iterable<HGQuery<Key>>
{
	private KeyBasedQuery<Key, Value> out;
	private HGQuery<Key> in;
	
	public PipeQuery(HGQuery<Key> in, KeyBasedQuery<Key, Value> out)
	{
		this.in = in;
		this.out = out;
	}
	
    public HGSearchResult<Value> execute() 
	{
		return new PipedResult<Key, Value>(in.execute(), out, true);
	}
	
    public Iterator<HGQuery<Key>> iterator()
    {
        return Arrays.asList(in).iterator();
    }
}