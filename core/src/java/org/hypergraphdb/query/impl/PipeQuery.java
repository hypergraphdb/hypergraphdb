/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query.impl;

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
public class PipeQuery extends HGQuery 
{
	private KeyBasedQuery out;
	private HGQuery in;
	
	public PipeQuery(HGQuery in, KeyBasedQuery out)
	{
		this.in = in;
		this.out = out;
	}
	
	public HGSearchResult execute() 
	{
		return new PipedResult(in.execute(), out, true);
	}
}