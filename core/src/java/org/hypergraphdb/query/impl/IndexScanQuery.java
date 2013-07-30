/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * This queries simply scans all elements in a given index. The result of
 * <code>execute</code> is actually <code>HGRandomAccessResult</code>. One can
 * scan either the keys or the values of the <code>HGIndex</code> based on
 * the <code>returnKeys</code> boolean constructor parameter.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class IndexScanQuery<Key, Value> extends HGQuery<Value> 
{
	private HGIndex<Key, Value> idx;
	private boolean returnKeys = false;
	
	public IndexScanQuery(HGIndex<Key, Value> idx, boolean returnKeys)
	{
		this.idx = idx;
		this.returnKeys = returnKeys;
	}
	
	@SuppressWarnings("unchecked")
    @Override
	public HGSearchResult execute() 
	{
		return returnKeys ? idx.scanKeys() : idx.scanValues();
	}
}