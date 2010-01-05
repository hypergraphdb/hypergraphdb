/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * A <code>HGProjection</code> represents a dimension of a composite type. The defining properties
 * of a projection are its name and its type. A composite type may have several projections with
 * the same name as long as they are of different types.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGProjection 
{
	/**
	 * <p>Return the name of the dimension that this projection represents.</p>
	 */
	String getName();

	/**
	 * <p>Return the handle of the type of the dimension that this projection represents.</p> 
	 */
	HGHandle getType();
	
	/**
	 * <p>Return the layout path in the <code>HGStore</code> from the composite
	 * value to the value along this dimension. May return <code>null</code> if the
	 * precise layout is not know, or not well-defined.
	 * </p>
	 * 
	 * <strong>TODO:</strong> not sure this is needed...
	 */
	int [] getLayoutPath();
	
	/**
	 * <p>Return the projection of the passed in value along the dimension 
	 * represented by this object.</p>
	 * 
	 * @param atomValue The value of the atom whose projection value is desired.
	 * @return The value along the dimension embedded within the <code>HGProjection</code>. 
	 */
	Object project(Object atomValue);
	
	/**
	 * <p>Modify an atom's projections.</p>
	 * 
	 * @param atomValue The value of the atom whose projection value is desired.
	 * @param The new value along the dimension embedded within the <code>HGProjection</code>. 
	 */
	void inject(Object atomValue, Object value);
}
