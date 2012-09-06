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
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * A simple query that operates on a single index.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class IndexBasedQuery<T> extends HGQuery<T>
{
    public static enum ScanType { none, keys, values };
    
    private HGIndex<? extends Object, ? extends Object> index;
    private Ref<Object> key;
    private ComparisonOperator operator = ComparisonOperator.EQ;
    private ScanType scanType = ScanType.none;
    

    
/*    public static final int EQ= 0;
    public static final int LT = 1;
    public static final int GT = 2;
    public static final int LTE = 3;
    public static final int GTE = 4; */
    
    /**
     * <p>
     * Construct a query that will scan the whole index - either its keys or its
     * values depending on the <code>scanKeys</code> parameter.
     * </p>
     * 
     * @param index The <code>HGIndex</code> on which the query is performed.
     * @param scanKeys <code>true</code> if all keys must be scanned and 
     * <code>false</code> if all values must be scanned instead.
     */
    public IndexBasedQuery(HGIndex<Object, Object> index, ScanType scanType)
    {
    	this.index = index;
    	this.scanType = scanType;
    }
    
    public IndexBasedQuery(HGIndex<? extends Object, ? extends Object> index, Object key)
    {
        this.index = index;
        this.key = hg.constant(key);
    }
    
    public IndexBasedQuery(HGIndex<Object, Object> index, Ref<Object> key, ComparisonOperator operator)
    {
    	this.index = index;
    	this.key = key;
    	this.operator = operator;    	
    }
    
    public IndexBasedQuery(HGIndex<Object, Object> index, Object key, ComparisonOperator operator)
    {
    	this(index, hg.constant(key), operator);
    }
    
    public HGSearchResult<T> execute()
    {
    	switch (scanType)
    	{
    		case keys: return ((HGIndex<T, ?>)index).scanKeys();
    		case values: ((HGIndex<?, T>)index).scanValues();
    		default:
		    	switch (operator)
		    	{
		    		case EQ:
		    			return ((HGIndex<Object, T>)index).find(key.get());
		    		case LT:
		    			return ((HGSortIndex<Object, T>)index).findLT(key.get());
		    		case GT:
		    			return ((HGSortIndex<Object, T>)index).findGT(key.get());
		    		case LTE:
		    			return ((HGSortIndex<Object, T>)index).findLTE(key.get());
		    		case GTE:
		    			return ((HGSortIndex<Object, T>)index).findGTE(key.get());   
		    		default:
		    			throw new HGException("Wrong operator code [" + operator + "] passed to IndexBasedQuery.");
		    	}
	    }
    }
}