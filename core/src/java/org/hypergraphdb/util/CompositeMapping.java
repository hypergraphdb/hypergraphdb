/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

public final class CompositeMapping<From, To> implements Mapping<From, To> 
{
	private Mapping<From, Object> first;
	private Mapping<Object, To> second;
	
	public CompositeMapping(Mapping<From, Object> first, Mapping<Object, To> second)
	{
		this.first = first;
		this.second = second;
	}
	
	public To eval(From x) 
	{
		return second.eval(first.eval(x));		
	}
}
