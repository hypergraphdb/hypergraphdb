/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

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
public class UnionQuery extends HGQuery 
{
	private HGQuery left, right;
	
	/**
	 * <p>Construct a union of two queries.</p>
	 * 
	 * @param left One of the two queries. May not be <code>null</code>.
	 * @param right The other of the two queries. May not be <code>null</code>.
	 */
	public UnionQuery(HGQuery left, HGQuery right)
	{
		this.left = left;
		this.right = right;
	}
	
	public HGSearchResult execute()
	{
		HGSearchResult leftResult = left.execute();
		HGSearchResult rightResult = right.execute();		
		if (!leftResult.hasNext() && !rightResult.hasNext())
		{
			leftResult.close();
			rightResult.close();
			return HGSearchResult.EMPTY;
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
}
