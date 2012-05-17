/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;
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
	
	public AtomPartCondition()
	{
		super(null, ComparisonOperator.EQ);
	}
	
  public AtomPartCondition(String [] dimensionPath, Object value)
  {
  	this (dimensionPath, value, ComparisonOperator.EQ);
  }
    
	public AtomPartCondition(String [] dimensionPath, 
						     Object value, 
						     ComparisonOperator operator)
	{
		this(dimensionPath, hg.constant(value), operator);
	}

	public AtomPartCondition(String [] dimensionPath, 
		     				 Ref<Object> value, 
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

	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		Object atom = null;
		atom = hg.get(handle);
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
        //System.err.println(ex);
    }

    if (projected != null)
			return compareToValue(hg, projected.getValue());
		else
			return false;
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("valueOf(");
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
	
	public int hashCode() 
	{ 
		return HGUtils.hashThem(dimensionPath, HGUtils.hashThem(value, operator));
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomPartCondition))
			return false;
		else
		{
			AtomPartCondition c = (AtomPartCondition)x;
			return HGUtils.eq(operator, c.operator) &&
				   HGUtils.eq(value, c.value) &&
				   HGUtils.eq(dimensionPath, c.dimensionPath);
		}
	}	
}
