/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGOrderedSearchable;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * A simple query that operates on a single <code>HGSearchable</code> entity, usually
 * a <code>HGIndex</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SearchableBasedQuery<Key, Value> extends KeyBasedQuery<Key, Value>
{
    protected HGSearchable<Key, Value> searchable;
    protected Ref<Key> key;    
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
    public SearchableBasedQuery(HGSearchable<Key, Value> searchable, Key key, ComparisonOperator operator)
    {
        this(searchable, hg.constant(key), operator);
    }
    
    public SearchableBasedQuery(HGSearchable<Key, Value> searchable, Ref<Key> key, ComparisonOperator operator)
    {
        this.searchable = searchable;
        this.key = key;
        this.operator = operator;    	
    }
    
    public HGSearchResult<Value> execute()
    {
        switch (operator)
        {
            case EQ:
                return searchable.find(key.get());
            case LT:
                return ((HGOrderedSearchable<Key, Value>)searchable).findLT(key.get());
            case GT:
                return ((HGOrderedSearchable<Key, Value>)searchable).findGT(key.get());
            case LTE:
                return ((HGOrderedSearchable<Key, Value>)searchable).findLTE(key.get());
            case GTE:
                return ((HGOrderedSearchable<Key, Value>)searchable).findGTE(key.get());   
            default:
                throw new HGException("Wrong operator code [" + operator + "] passed to IndexBasedQuery.");
        }
    }
    
    public void setKeyReference(Ref<Key> key)
    {
    	this.key = key;
    }
    
    public Ref<Key> getKeyReference()
    {
    	return key;
    }
    
    public void setKey(Key key)
    {
    	this.key = hg.constant(key);
    }
    
    public Key getKey()
    {
    	return key.get();
    }
    
    public void setOperator(ComparisonOperator operator)
    {
    	this.operator = operator;
    }
    
    public ComparisonOperator getOperator()
    {
    	return operator;
    }
    
    public void setSearchable(HGSearchable<Key, Value> searchable)
    {
    	this.searchable = searchable;
    }
    
    public HGSearchable<Key, Value> getSearchable()
    {
    	return searchable;
    }
}