/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.query.ComparisonOperator;

/**
 * <p>
 * A simple query that operates on a single index.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class IndexBasedQuery extends HGQuery
{
    private HGIndex index;
    private Object key;
    private ComparisonOperator operator = ComparisonOperator.EQ;
    
/*    public static final int EQ= 0;
    public static final int LT = 1;
    public static final int GT = 2;
    public static final int LTE = 3;
    public static final int GTE = 4; */
    
    public IndexBasedQuery(HGIndex index, Object key)
    {
        this.index = index;
        this.key = key;
    }
    
    public IndexBasedQuery(HGSortIndex index, Object key, ComparisonOperator operator)
    {
    	this.index = index;
    	this.key = key;
    	this.operator = operator;
    }
    
    public HGRandomAccessResult execute()
    {
    	switch (operator)
    	{
    		case EQ:
    			return index.find(key);
    		case LT:
    			return ((HGSortIndex)index).findLT(key);
    		case GT:
    			return ((HGSortIndex)index).findGT(key);
    		case LTE:
    			return ((HGSortIndex)index).findLTE(key);
    		case GTE:
    			return ((HGSortIndex)index).findGTE(key);   
    		default:
    			throw new HGException("Wrong operator code [" + operator + "] passed to IndexBasedQuery.");
    	}
    }
}