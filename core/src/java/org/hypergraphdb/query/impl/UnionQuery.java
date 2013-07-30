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
 * An <code>UnionQuery</code> combines the results of two underlying
 * queries and produces a result set containing elements that appear
 * in either of the input result sets.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class UnionQuery<T> extends HGQuery<T> implements Iterable<HGQuery<T>>
{
	private HGQuery<T> left, right;
	
	/**
	 * <p>Construct a union of two queries.</p>
	 * 
	 * @param left One of the two queries. May not be <code>null</code>.
	 * @param right The other of the two queries. May not be <code>null</code>.
	 */
	public UnionQuery(HGQuery<T> left, HGQuery<T> right)
	{
		this.left = left;
		this.right = right;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    public HGSearchResult<T> execute()
	{
		HGSearchResult<T> leftResult = left.execute();
		HGSearchResult<T> rightResult = right.execute();		
		if (!leftResult.hasNext() && !rightResult.hasNext())
		{
			leftResult.close();
			rightResult.close();
			return (HGSearchResult)HGSearchResult.EMPTY;
		}
		else if (!leftResult.hasNext())
		{
			leftResult.close();
			return rightResult;
		}
		else if (!rightResult.hasNext())
		{
			rightResult.close();
			return leftResult;
		}
		else
			return new UnionResult(leftResult, rightResult);
	}
	
    @SuppressWarnings("unchecked")
    public Iterator<HGQuery<T>> iterator()
    {
        return Arrays.asList(left,right).iterator();
    }	
}
