/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.indexing;


import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.type.AtomRefType;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGPrimitiveType;
import org.hypergraphdb.type.HGProjection;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * Represents by the value of a part in a composite type.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class ByPartIndexer<KeyType> extends HGKeyIndexer<KeyType>
{
	private String [] dimensionPath;
	private HGProjection [] projections = null;
	private HGAtomType projectionType = null;
 
	private synchronized HGProjection [] getProjections(HyperGraph graph)
	{
		if (projections == null)
		{
			HGAtomType type = graph.getTypeSystem().getType(getType());
			if (type == null)
				throw new HGException("Could not find type with handle " + getType());
			projections = new HGProjection[dimensionPath.length];
			for (int j = 0; j < dimensionPath.length; j++)
			{
				if (! (type instanceof HGCompositeType))
					return null;
				projections[j] = ((HGCompositeType)type).getProjection(dimensionPath[j]);
				if (projections[j] == null)
					throw new HGException("There's no projection '" + 
										  dimensionPath[j] + 
										  "' in type '" + type + "'");
				type = (HGAtomType)graph.get(projections[j].getType());
			}	
			HGProjection ours = projections[dimensionPath.length - 1];						
			HGHandle enclosingType = dimensionPath.length > 1 ?
					projections[dimensionPath.length - 2].getType() : getType();
			// For HGAtomRef's, we want to index by the atom directly:
    		HGHandle atomProj = hg.findOne(graph,
    				hg.and(hg.type(AtomProjection.class), 
    					   hg.incident(enclosingType),
    					   hg.incident(ours.getType()),
    					   hg.eq("name", ours.getName())));
    		if (atomProj != null)
    			projectionType = graph.getTypeSystem().getAtomType(HGAtomRef.class);
    		else 
    			projectionType = graph.get(ours.getType());
		}
		return projections;
	}
	
	public ByPartIndexer()
	{		
	}
	
	/**
	 * <p>
	 * Convenience constructor that allows passing a dot separated dimension path
	 * that is converted to a <code>String[]</code>. 
	 * </p>
	 * 
	 * @param type The type of the atoms to be indexed.
	 * @param dimensionPath The dimension path in dot format (e.g. "person.address.street")
	 */
	public ByPartIndexer(HGHandle type, String dimensionPath)
	{
		this(type, dimensionPath.split("\\."));
	}
	
	/**
	 * <p>
	 * Convenience constructor that allows passing a dot separated dimension path
	 * that is converted to a <code>String[]</code>. 
	 * </p>
	 * 
	 * @param name The name of the index.
	 * @param type The type of the atoms to be indexed.
	 * @param dimensionPath The dimension path in dot format (e.g. "person.address.street")
	 */
	public ByPartIndexer(String name, HGHandle type, String dimensionPath)
	{
		this(name, type, dimensionPath.split("\\."));
	}
	
	public ByPartIndexer(HGHandle type, String [] dimensionPath)
	{
		super(type);
		this.dimensionPath = dimensionPath;
	}

	public ByPartIndexer(String name, HGHandle type, String [] dimensionPath)
	{
		super(name, type);
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

	@SuppressWarnings("unchecked")
	public Comparator<byte[]> getComparator(HyperGraph graph)
	{
//		return null;  //is this Ok???
		if (projectionType == null)
			getProjections(graph);
		if (projectionType.getClass().equals(AtomRefType.class))
			return null;
		else if (projectionType instanceof HGPrimitiveType)
			return ((HGPrimitiveType<?>)projectionType).getComparator();
		else if (projectionType instanceof Comparator)
			return (Comparator<byte[]>)projectionType;
		else
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public ByteArrayConverter<KeyType> getConverter(HyperGraph graph)
	{
		if (projectionType == null)
			getProjections(graph);
		return (ByteArrayConverter<KeyType>)projectionType;
	}
	
	@SuppressWarnings("unchecked")
	public KeyType getKey(HyperGraph graph, Object atom)
	{
		Object result = atom;
		if (atom instanceof HGValueLink)
		    result = ((HGValueLink)atom).getValue();
		for (HGProjection p : getProjections(graph))
			result = p.project(result);
		return (KeyType)result;
	}
	
	@SuppressWarnings("unchecked")
	public boolean equals(Object other)
	{
		if (other == this)
			return true;
		if (! (other instanceof ByPartIndexer))
			return false;
		ByPartIndexer<KeyType> idx = (ByPartIndexer<KeyType>)other;
		return getType().equals(idx.getType()) && HGUtils.eq(dimensionPath, idx.dimensionPath);
	}
	
	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + HGUtils.hashIt(dimensionPath);
		hash = 31 * hash + getType().hashCode();
    return hash;
	}	
}
