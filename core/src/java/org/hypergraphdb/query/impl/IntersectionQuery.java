/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * An <code>IntersectionQuery</code> combines the results of two underlying
 * queries and produces a result set containing only elements that appear
 * in both of the input result sets.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class IntersectionQuery<T> extends HGQuery<T> implements Iterable<HGQuery<T>>
{	
	private HGQuery<T> left, right;
	private RSCombiner<T> combiner;
	
	/**
	 * <p>Construct an intersection of two queries.</p>
	 * 
	 * @param left One of the two queries. May not be <code>null</code>.
	 * @param right The other of the two queries. May not be <code>null</code>.
	 */
	public IntersectionQuery(HGQuery<T> left, HGQuery<T> right, RSCombiner<T> combiner)
	{
		this.left = left;
		this.right = right;
		this.combiner = combiner;
	}
	
	public HGSearchResult<T> execute()
	{
		//combiner.reset();
		HGSearchResult<T> leftResult = left.execute();
		HGSearchResult<T> rightResult = right.execute();
		if (!leftResult.hasNext() || !rightResult.hasNext())
		{
			leftResult.close();
			rightResult.close();
			return (HGSearchResult<T>)HGSearchResult.EMPTY;
		}
	//	return new SortedIntersectionResult(leftResult, rightResult);
//		combiner.init(leftResult, rightResult);
//		return combiner;
		return combiner.combine(leftResult, rightResult);
	}

    public HGQuery<T> getLeft()
    {
        return left;
    }

    public void setLeft(HGQuery<T> left)
    {
        this.left = left;
    }

    public HGQuery<T> getRight()
    {
        return right;
    }

    public void setRight(HGQuery<T> right)
    {
        this.right = right;
    }

    public RSCombiner<T> getCombiner()
    {
        return combiner;
    }

    public void setCombiner(RSCombiner<T> combiner)
    {
        this.combiner = combiner;
    } 

    public Iterator<HGQuery<T>> iterator()
    {
        return Arrays.asList(left, right).iterator();
    }
}