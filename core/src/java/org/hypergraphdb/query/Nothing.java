/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;


/**
 * <p>
 * This condition represents the negation of everything. It will yield an empty result set
 * alone or in conjunction with any other condition. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public final class Nothing implements HGQueryCondition 
{
	public static final Nothing Instance = new Nothing();
	
	/**
	 * this is required to ensure the class is a bean.
	 */
	public Nothing() { }
	public int hashCode() { return 0; }
	public boolean equals(Object x) { return x instanceof Nothing; }	
}
