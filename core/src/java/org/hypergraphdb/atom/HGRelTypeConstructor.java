/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGProjection;


/**
 * <p>
 * The type of <code>HGRelType</code>. Even though we could have treated 
 * <code>HGRelType</code>, that would have created different <code>RecordType</code>
 * which would have made it harder working purely with <code>HGRel</code>ationships
 * outside of Java (since one would have to get to the <code>RecordType</code> by
 * querying on the class name). In any case, a <code>HGRelType</code> is conceptually
 * not really a record...
 * </p>
 */
public class HGRelTypeConstructor implements HGSearchable<HGRelType, HGPersistentHandle>, HGCompositeType
{
    public static final String INDEX_NAME = "hg_reltype_value_index";
    
    private HyperGraph graph = null;
    private HGSortIndex<byte[], HGPersistentHandle> valueIndex = null;
    private Map<String, HGProjection> projections = new HashMap<String, HGProjection>();
    
    private void initProjections()
    {
    	projections.put("name", new HGProjection()
    	{

			public int[] getLayoutPath()
			{
				return new int[0];
			}

			public String getName()
			{
				return "name";
			}

			public HGHandle getType()
			{
				return graph.getTypeSystem().getTypeHandle(String.class);
			}

			public void inject(Object atomValue, Object value)
			{
				((HGRelType)atomValue).setName((String)value);
			}

			public Object project(Object atomValue)
			{
				return ((HGRelType)atomValue).getName();
			}    		
    	}
    	);
    }
    
    private static class ByteComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;

		public int compare(byte [] left, byte [] right)
		{ 
			return new String(left).compareTo(new String(right));
		}    	
    }
    
    private final HGSortIndex<byte[], HGPersistentHandle> getIndex()
    {
        if (valueIndex == null)
        {
            valueIndex = (HGSortIndex<byte[], HGPersistentHandle>)graph.getStore().getIndex(INDEX_NAME, 
            																			 BAtoBA.getInstance(), 
            																			 BAtoHandle.getInstance(graph.getHandleFactory()),
            																			 new ByteComparator(),
            																			 true);
        }
        return valueIndex;
    }
    
	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{
		HGAtomType sType = graph.getTypeSystem().getAtomType(String.class);
		String name = (String)sType.make(handle, null, null);
		return new HGRelType(name, targetSet.deref());
	}

	public HGPersistentHandle store(Object instance)
	{
		HGRelType relType = (HGRelType)instance;
		HGAtomType sType = graph.getTypeSystem().getAtomType(String.class);
		HGPersistentHandle result = sType.store(relType.getName());
		if (getIndex().findFirst(relType.getName().getBytes()) == null)
			getIndex().addEntry(relType.getName().getBytes(), result);
		return result;
	}

	public void release(HGPersistentHandle handle)
	{
		HGAtomType sType = graph.getTypeSystem().getAtomType(String.class);
		String s = (String)sType.make(handle, null, null);
		sType.release(handle);
		getIndex().removeEntry(s.getBytes(), handle);
	}

	
	/**
	 * <p>
	 * A <code>HGRelType</code> <em>X</em> subsumes a <code>HGRelType</code> <em>Y</em>
	 * iff both have the same name and arity and each target atom of <em>X</em> subsumes
	 * the corresponding target atom of <em>Y</em>. 
	 * </p>
	 * <p>
	 * In plain language this reflects the logical requirement that each instance 
	 * relationship with type <em>Y</em> be also an instance (logically) of <em>X</em>. 
	 * </p>
	 * 
	 */
	public boolean subsumes(Object general, Object specific)
	{
		HGRelType grel = (HGRelType)general;
		HGRelType srel = (HGRelType)specific;
		if (general == null || specific == null)
			return general == specific;
		if (!grel.getName().equals(srel.getName()) || grel.getArity() != srel.getArity())
			return false;
		for (int i = 0; i < grel.getArity(); i++)
		{
			HGHandle g = grel.getTargetAt(i);
			HGHandle s = srel.getTargetAt(i);
			if (g.equals(s))
				continue;			
			else
			{
				HGAtomType gt = graph.getTypeSystem().getAtomType(g);
				HGAtomType st = graph.getTypeSystem().getAtomType(s);
				if (!gt.equals(st) || !gt.subsumes(graph.get(g), graph.get(s)))
					return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public HGSearchResult<HGPersistentHandle> find(HGRelType key)
	{
		if (key == null || key.getName() == null)
			return (HGSearchResult<HGPersistentHandle>)HGSearchResult.EMPTY;
		
		return getIndex().find(key.getName().getBytes());
	}

	public Iterator<String> getDimensionNames()
	{
		return projections.keySet().iterator();
	}

	public HGProjection getProjection(String dimensionName)
	{
		return projections.get(dimensionName);
	}

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
		initProjections();
	}		
}