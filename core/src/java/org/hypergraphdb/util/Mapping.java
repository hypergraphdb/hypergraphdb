/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

/**
 * <p>An interface specifying a single argument function.</p> 
 */
public interface Mapping<From, To>
{
	/**
	 * <p>Map the parameter x and produce a result. The mapping is completely
	 * arbitrary and depending on the context under which it is defined. This
	 * interface mandates no restriction whatsoever as far the input or output
	 * of this mapping are concerned.</p>
	 * 
	 * @param x The mapping input. 
	 * @return The mapping output.
	 */
	To eval(From x);
}
