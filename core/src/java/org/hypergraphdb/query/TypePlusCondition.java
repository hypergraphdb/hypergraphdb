/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGDepthFirstTraversal;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.util.HGUtils;

public class TypePlusCondition implements HGQueryCondition, HGAtomPredicate
{
	private Class<?> clazz;
	private HGHandle baseType;
	private List<HGHandle> subTypes = null;
	
	private void fetchSubTypes(HyperGraph graph)
	{
		if (baseType == null)
			baseType = graph.getTypeSystem().getTypeHandle(clazz);		
		DefaultALGenerator alGenerator = new DefaultALGenerator(graph, 
																new AtomTypeCondition(HGSubsumes.class),											
												                null,
												                false,
												                true,
												                false);
		HGDepthFirstTraversal traversal = new HGDepthFirstTraversal(baseType, alGenerator);
		subTypes = new ArrayList<HGHandle>();
		while (traversal.hasNext())
			subTypes.add(traversal.next().getSecond());
		// add it at the end since presumably this would be a base, abstract type most
		// of the time, so it should be checked last.
		subTypes.add(baseType);		
	}
	
	public TypePlusCondition()
	{
		
	}
	public TypePlusCondition(HGHandle baseType)
	{
		this.baseType = baseType;
		if (baseType == null)
			throw new NullPointerException("Base type is null in TypePlusCondition!");
	}
	
	public TypePlusCondition(Class<?> clazz)
	{
		this.clazz = clazz;
		if (clazz == null)
			throw new NullPointerException("Base type is null in TypePlusCondition!");
	}
	
	public boolean satisfies(HyperGraph graph, HGHandle handle)
	{
		HGHandle tpHdl = graph.getType(handle);
		
		for (HGHandle t : getSubTypes(graph))
			if (tpHdl.equals(t))
				return true;
		return false;
	}
	
	public void setBaseType(HGHandle baseType)
	{
		this.baseType = baseType;
	}
	
	public HGHandle getBaseType()
	{
		return baseType;
	}

	public void setJavaClass(Class<?> javaClass)
	{
		this.clazz = javaClass;
	}
	
	public Class<?> getJavaClass()
	{
		return clazz;
	}
	
	public List<HGHandle> getSubTypes(HyperGraph graph)
	{
		if (subTypes == null)
			fetchSubTypes(graph);
		return subTypes;
	}
	
	public int hashCode() 
	{ 
		return clazz == null ? baseType.hashCode() : clazz.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof TypePlusCondition))
			return false;
		else
		{
			TypePlusCondition c = (TypePlusCondition)x;
			return clazz == null ? HGUtils.eq(baseType, c.baseType) : HGUtils.eq(clazz, c.clazz);
		}
	}	
	
	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("TypePlusCondition, ");
		
		if (clazz != null) {
			result.append("clazz:");
			result.append(clazz.getName());
			result.append(", ");
		}
		
		result.append("baseType:");
		result.append(baseType);
		result.append(", subTypes(");

		if (subTypes == null) {
			result.append("<null>");
		}
		else {
			boolean first = true;
			for (HGHandle t : subTypes) {
				if (!first) {
					result.append(",");
				}
				result.append(t);
				first = false;
			}
		}
		result.append(")");
		return result.toString();
	}
}
