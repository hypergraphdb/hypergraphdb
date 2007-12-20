package org.hypergraphdb.indexing;

import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
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
public class ByPartIndexer extends HGIndexer
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
			projectionType = (HGAtomType)graph.get(projections[projections.length - 1].getType());
		}
		return projections;
	}
	
	public ByPartIndexer()
	{		
	}
	
	public ByPartIndexer(HGHandle type, String [] dimensionPath)
	{
		super(type);
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

	@Override
	public Comparator getComparator(HyperGraph graph)
	{
		if (projectionType == null)
			getProjections(graph);
		return (Comparator)projectionType;
	}

	@Override
	public ByteArrayConverter getConverter(HyperGraph graph)
	{
		if (projectionType == null)
			getProjections(graph);
		return (ByteArrayConverter<Object>)projectionType;
	}

	@Override
	public Object getKey(HyperGraph graph, Object atom)
	{
		Object result = atom;
		for (HGProjection p : getProjections(graph))
			result = p.project(result);
		return result;
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == this)
			return true;
		if (! (other instanceof ByPartIndexer))
			return false;
		ByPartIndexer idx = (ByPartIndexer)other;
		return getType().equals(idx.getType()) && HGUtils.eq(dimensionPath, idx.dimensionPath);
	}

	@Override
	public int hashCode()
	{
		return getType().hashCode();
	}	
}