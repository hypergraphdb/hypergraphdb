/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGOrderedSearchable;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.ComparisonOperator;

/**
 * <p>
 * A simple query that operates on a single <code>HGSearchable</code> entity, usually
 * a <code>HGIndex</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SearchableBasedQuery extends KeyBasedQuery
{
    protected HGSearchable searchable;
    protected Object key;    
    private ComparisonOperator operator = ComparisonOperator.EQ;
    
    /**
     * <p>
     * Construct a new <code>HGSearchable</code> based query. 
     * </p>
     * 
     * @param searchable The instance that will be searched.
     * @param key The search key.
     * @param operator A <code>ComparisonOperator</code> for the search. If it is
     * something else than a <code>ComparisonOperator.EQ</code> operator, it must be
     * supported by the concrete <code>HGSearchable</code> object passed. For instance,
     * an order operator like <code>ComparisonOperator.LT</code> and the like requires
     * a <code>HGOrderedSearchable</code> instance.
     */
    public SearchableBasedQuery(HGSearchable searchable, Object key, ComparisonOperator operator)
    {
        this.searchable = searchable;
        this.key = key;
        this.operator = operator;
    }
    
    
    public HGSearchResult execute()
    {
        switch (operator)
        {
            case EQ:
                return searchable.find(key);
            case LT:
                return ((HGOrderedSearchable)searchable).findLT(key);
            case GT:
                return ((HGOrderedSearchable)searchable).findGT(key);
            case LTE:
                return ((HGOrderedSearchable)searchable).findLTE(key);
            case GTE:
                return ((HGOrderedSearchable)searchable).findGTE(key);   
            default:
                throw new HGException("Wrong operator code [" + operator + "] passed to IndexBasedQuery.");
        }
    }
    
    public void setKey(Object key)
    {
    	this.key = key;
    }
    
    public Object getKey()
    {
    	return key;
    }
    
    public void setOperator(ComparisonOperator operator)
    {
    	this.operator = operator;
    }
    
    public ComparisonOperator getOperator()
    {
    	return operator;
    }
    
    public void setSearchable(HGSearchable searchable)
    {
    	this.searchable = searchable;
    }
    
    public HGSearchable getSearchable()
    {
    	return searchable;
    }
}