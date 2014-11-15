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
 * <p>
 * Represents a name relationship/link between entities.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGRel extends HGPlainLink 
{
	private String name;
	
	public HGRel(HGHandle...targetSet)
	{
		super(targetSet);
		this.name = "<name unavailable>";
	}
	
	public HGRel(String name, HGHandle...targetSet)
	{
		super(targetSet);
		this.name = name;
	}

	public String getName() 
	{
		return name;
	}
	
	public String toString()
	{
		return name + "[" + getArity() + "]";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HGRel other = (HGRel) obj;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;
		if (getArity() != other.getArity())
			return false;
		for (int i = 0; i < getArity(); i++)
			if (!getTargetAt(i).equals(other.getTargetAt(i)))
				return false;
		return true;
	}
	
}
