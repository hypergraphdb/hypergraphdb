/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

/**
 * 
 * <p>
 * This link represents a relationship between a composite type and one of its
 * projections. It states that the projection in question is an atom in itself 
 * and therefore must be recorded as a <code>HGAtomRef</code> in all values
 * of this composite type. The <code>mode</code> of the atom reference projection
 * is the sole attribute of the relationship.
 * </p>
 *
 * <p>
 * The link is between the type whose projection is an atom reference and
 * the type of the projection's value. 
 * </p>
 * 
 * <p>
 * An <code>AtomProjection</code> also holds the name of the projection as an attribute.
 * the name together with the value type are enough (and generally necessary) to identify
 * the projection.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class AtomProjection extends HGPlainLink
{
	private HGAtomRef.Mode mode;
	private String name;
	
	public AtomProjection(HGHandle [] targetSet)
	{
		super(targetSet);
	}
	
	/**
	 * <p>
	 * Construct an atom projection link.
	 * </p>
	 * 
	 * @param type The handle to a <code>HGCompositeType</code>.
	 * @param name The name of the projection.
	 * @param valueType The type of the projection's value. 
	 * @param mode The mode of the atom reference to be used when managing atoms
	 * of the composite type.
	 */
	public AtomProjection(HGHandle type, String name, HGHandle valueType, HGAtomRef.Mode mode)
	{
		super(new HGHandle[] {type, valueType});
		this.mode = mode;
		this.name = name;
	}
	
	public HGHandle getType()
	{
		return getTargetAt(0);
	}
	
	public HGHandle getProjectionValueType()
	{
		return getTargetAt(1);
	}
	
	public HGAtomRef.Mode getMode()
	{
		return mode;
	}
	
	public void setMode(HGAtomRef.Mode mode)
	{
		this.mode =  mode;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}	
}
