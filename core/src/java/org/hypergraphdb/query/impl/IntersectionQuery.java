/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
/*
 * Created on Aug 10, 2005
 *
 */
package org.hypergraphdb.query.impl;

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
public class IntersectionQuery extends HGQuery 
{
	private HGQuery left, right;
	private RSCombiner combiner;
	
	/**
	 * <p>Construct an intersection of two queries.</p>
	 * 
	 * @param left One of the two queries. May not be <code>null</code>.
	 * @param right The other of the two queries. May not be <code>null</code>.
	 */
	public IntersectionQuery(HGQuery left, HGQuery right, RSCombiner combiner)
	{
		this.left = left;
		this.right = right;
		this.combiner = combiner;
	}
	
	public HGSearchResult execute()
	{
		HGSearchResult leftResult = left.execute();
		HGSearchResult rightResult = right.execute();
		if (!leftResult.hasNext() || !rightResult.hasNext())
		{
			leftResult.close();
			rightResult.close();
			return HGSearchResult.EMPTY;
		}
	//	return new SortedIntersectionResult(leftResult, rightResult);
		combiner.init(leftResult, rightResult);
		return combiner;
	}
}