/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

/**
 * <p>
 * A <code>HGCompositeType</code> represents a HyperGraph type with several
 * dimensions. Each dimension has a name and is represented by a <code>HGProjection</code>. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGCompositeType extends HGAtomType
{
	/**
	 * <p>Return an <code>Iterator</code> listing the names of all 
	 * dimensions of this composite type. A projection function can be obtained
	 * through the <code>getProjection</code> method.
	 * </p> 
	 */
	java.util.Iterator<String> getDimensionNames();
	
	/**
	 * <p>Get the projection function for a particular dimension.</p>
	 * 
	 * @param dimensionName The name of the dimension.
	 * @return The <code>HGProjection</code> instance.
	 */
	HGProjection getProjection(String dimensionName);
}
