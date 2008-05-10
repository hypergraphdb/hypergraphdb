/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.util;

/**
 * <p>
 * The inexplicably missing class from the <code>java.util</code>. A generic, immutable <code>Pair</code>
 * class. Hash code computed at construction time and cached.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class Pair<F, S> 
{
	int hash;
	private F first;
	private S second;
	
	public Pair(F first, S second)
	{
		this.first = first;
		this.second = second;
		
        hash = (first != null) ? first.hashCode() : 0;
		if (second != null)
			hash ^= second.hashCode();		
	}
	
	public F getFirst()
	{
		return first;
	}
	
	public S getSecond()
	{
		return second;
	}
		
	public int hashCode()
	{
		return hash;
	}
	
	public boolean equals(Object other)
	{
		if (! (other instanceof Pair))
			return false;
		
		Pair<?,?> p = (Pair<?,?>)other;
		
		if (first == null)
			{ if (p.first != null) return false; }
		else if (p.first == null)
			return false;
		else if (!first.equals(p.first)) 
			return false;
		
		if (second == null) 
			return p.second == null;
		else if (p.second == null)
			return false;
		else
			return second.equals(p.second);
	}
	
	public String toString()
	{
		return "pair(" + first + "," + second + ")";
	}
}