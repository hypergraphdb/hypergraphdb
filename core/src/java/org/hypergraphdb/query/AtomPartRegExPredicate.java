/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import java.util.regex.Pattern;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;

/**
 * A predicate that constrains the value of a component of a composite typed atom using a
 * regular expression.
 * Analogous to the <code>AtomValueRegExCondition</code>, but operates on properties (parts)
 * of values.
 * 
 * @author Niels Beekman
 */
public class AtomPartRegExPredicate extends AtomRegExPredicate 
{
	private String [] dimensionPath;
	
	public AtomPartRegExPredicate()
	{
		super ();
	}
	
    public AtomPartRegExPredicate(String [] dimensionPath, Pattern pattern)
    {
        super (pattern);
        this.dimensionPath = dimensionPath;
    }

	public String[] getDimensionPath()
	{
		return dimensionPath;
	}

	public void setDimensionPath(String[] dimensionPath)
	{
		this.dimensionPath = dimensionPath;
	}

	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		Object atom = hg.get(handle);
		if (atom == null)
			return false;
		HGHandle type = hg.getTypeSystem().getTypeHandle(handle);			
        HGTypedValue projected = null;
        try
        {
            projected = TypeUtils.project(hg, type, atom, dimensionPath, false);
        }
        catch (IllegalArgumentException ex)
        {
            // projected remain null in case we have a problem projecting that value
        }
		if (projected != null)
			return satisfies(projected.getValue());
		else
			return false;
	}
	
	public String toString()
	{
		StringBuilder result = new StringBuilder("regEx(");
		if (dimensionPath != null)
		{
			for (int i = 0; i < dimensionPath.length; i++)
			{
				result.append(dimensionPath[i]);
				if (i + 1 < dimensionPath.length)
					result.append(".");
			}
			result.append(", ");
		}
		result.append(getPattern());
		result.append(")");
		return result.toString();
	}	
	
	public int hashCode() 
	{ 
		return HGUtils.hashIt(dimensionPath) | super.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomPartRegExPredicate))
			return false;
		if (! super.equals(x))
			return false;
		else
		{
			AtomPartRegExPredicate c = (AtomPartRegExPredicate)x;
			return HGUtils.eq(dimensionPath, c.dimensionPath);
		}
	}	
}
