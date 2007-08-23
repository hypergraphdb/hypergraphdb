/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query;

import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGHandle;

/**
 * <p>
 * A condition that constraints the value of a component of a composite typed atom.
 * Analogous to the <code>AtomValueCondition</code>, but operates on properties (parts)
 * of values.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class AtomPartCondition extends SimpleValueCondition 
{
	private String [] dimensionPath;
	
    public AtomPartCondition(String [] dimensionPath, Object value)
    {
        this (dimensionPath, value, ComparisonOperator.EQ);
    }
    
	public AtomPartCondition(String [] dimensionPath, 
						     Object value, 
						     ComparisonOperator operator)
	{
		super(value, operator);
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

	protected boolean satisfies(HyperGraph hg, HGHandle atomHandle, Object atom, HGHandle type) 
	{
		HGTypedValue projected = TypeUtils.project(hg, type, atom, dimensionPath, false);
		if (projected != null)
			return compareToValue(hg, projected.getValue(), projected.getType());
		else
			return false;
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("valufOf(");
		for (int i = 0; i < dimensionPath.length; i++)
		{
			result.append(dimensionPath[i]);
			if (i + 1 < dimensionPath.length)
				result.append(".");
		}
		result.append(", ");
		result.append(getOperator());
		result.append(",");
		result.append(String.valueOf(getValue()));
		result.append(")");
		return result.toString();
	}	
}