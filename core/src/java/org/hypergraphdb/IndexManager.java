/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Comparator;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGProjection;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.BAtoHandle;

/**
 * <p>
 * Manages property-based indices for composite types.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
class IndexManager 
{
	private static final String COMPINDEX_FILENAME ="hg_compindex_list";
	
	private HyperGraph hg;
	private Map<HGPersistentHandle, List<CompositeIndex>> typeToIndices = 
		new HashMap<HGPersistentHandle, List<CompositeIndex>>();
	
	// A simple structure to hold the run-time representation of the index
	// of a part within a composite type.
	private static final class CompositeIndex 
	{ 
		public String [] dimPath;
		public HGIndex<Object, HGPersistentHandle> idx; 
		public CompositeIndex(String [] dimPath, HGIndex<Object, HGPersistentHandle> idx) 
		{ this.dimPath = dimPath; this.idx = idx; }
	}
	
	private void loadIndices()
	{
        try
        {
            File indicesFile = new File(hg.getStore().getDatabaseLocation(), COMPINDEX_FILENAME);
            if (indicesFile.exists())
            {
                FileReader freader = new FileReader(indicesFile);
                BufferedReader reader = new BufferedReader(freader);
                for (String line = reader.readLine(); line != null; line = reader.readLine())
                {
                	StringTokenizer tok = new StringTokenizer(line, " ");
                	HGPersistentHandle typeHandle = HGHandleFactory.makeHandle(tok.nextToken());
                	while (tok.hasMoreTokens())
                	{
                		String [] dimPath = TypeUtils.parseDimensionPath(tok.nextToken());
                		String store_idx_name = makeIndexName(typeHandle, dimPath);
                		HGProjection proj = TypeUtils.getProjection(hg, hg.getTypeSystem().getType(typeHandle), dimPath);
                		ByteArrayConverter bac = (ByteArrayConverter)hg.get(proj.getType());
                		HGIndex<Object, HGPersistentHandle> idx = 
                			hg.getStore().getIndex(store_idx_name, bac, BAtoHandle.getInstance(), null);
                		if (idx == null)
                			throw new HGException("Fatal error - could not load index " + store_idx_name 
                					+ " because it has been removed from the store.");
                		add(typeHandle, new CompositeIndex(dimPath, idx));
                	}
                }
                reader.close();
                freader.close();
            }
        }
        catch (Throwable t)
        {
            throw new HGException("Could not read indices list file: " + t.toString(), t);
        }		
	}
	
	private void saveIndices()
	{
        try
        {
            File indicesFile = new File(hg.getStore().getDatabaseLocation(), COMPINDEX_FILENAME);
            FileWriter out = new FileWriter(indicesFile);
            for (Iterator i = typeToIndices.keySet().iterator(); i.hasNext(); )
            {
            	HGPersistentHandle type = (HGPersistentHandle)i.next();
            	List indices = (List)typeToIndices.get(type);
            	out.write(type.toString());
            	out.write(" ");
            	for (Iterator j = indices.iterator(); j.hasNext(); )
	            {
	                CompositeIndex idx = (CompositeIndex)j.next();
	                out.write(TypeUtils.formatDimensionPath(idx.dimPath));
                    out.write(" ");
	            }
                out.write("\n");            	
            }
           out.close();
        }
        catch (Throwable t)
        {
            throw new HGException("Could not write index list file: " + t.toString(), t);
        }		
	}

	
	/**
	 * Construct a name for the index.
	 */
	private String makeIndexName(HGPersistentHandle handle, String [] dimensionPath)
	{
		StringBuffer result = new StringBuffer();
		result.append(handle.toString());
		for (int i = 0; i  < dimensionPath.length; i++)
		{
			result.append("_");
			result.append(result.append(dimensionPath[i]));
		}
		return result.toString();
	}

	private void add(HGPersistentHandle typeHandle, CompositeIndex ci)
	{
		List<CompositeIndex> l = typeToIndices.get(typeHandle);
		if (l == null)
		{
			l = new ArrayList<CompositeIndex>();
			typeToIndices.put(typeHandle, l);
		}
		l.add(ci);
	}
	
	public IndexManager(HyperGraph hg)
	{
		this.hg = hg;
		loadIndices();
	}
	
	public void close()
	{
		saveIndices();
	}
	
	public HGIndex<?, HGPersistentHandle> createIndex(HGPersistentHandle typeHandle, String [] dimensionPath)
	{
		HGAtomType type = hg.getTypeSystem().getType(typeHandle);
		if (type == null)
			throw new HGException("Could not find type with handle " + typeHandle);
		
		HGProjection proj = TypeUtils.getProjection(hg, type, dimensionPath);
		if (proj == null)
			throw new HGException("Could not obtain projection " + TypeUtils.formatDimensionPath(dimensionPath) +
								  " on type " + typeHandle + " - " + type.getClass().getName());
		
		HGAtomType projType = hg.getTypeSystem().getType(proj.getType());
		if (! (projType instanceof ByteArrayConverter && projType instanceof Comparator))
			throw new HGException("Cannot create index for type " + 
								   typeHandle + 
								   " and dimension path " +
								   TypeUtils.formatDimensionPath(dimensionPath) +
								   " since the type of the projection along that dimension does not implement " +
								   "both the org.hypergraphdb.storage.ByteArrayConverter and java.util.Comparator interfaces.");
		String name = makeIndexName(typeHandle, dimensionPath);
		HGIndex<?, HGPersistentHandle> idx = hg.getStore().getIndex(name, 
							                                       (ByteArrayConverter)projType, 
							                                       BAtoHandle.getInstance(), 
							                                       (Comparator)projType);
		if (idx != null)
			throw new HGException("Index for type " + typeHandle + 
								  " and dimension path " +
								  TypeUtils.formatDimensionPath(dimensionPath) + 
								  " already exists!");

		HGIndex<Object, HGPersistentHandle> newIndex = 
			hg.getStore().createIndex(name, 
                                     (ByteArrayConverter)projType, 
                                     BAtoHandle.getInstance(), 
                                     (Comparator)projType);
		CompositeIndex ci = new CompositeIndex(dimensionPath, newIndex);
		add(typeHandle, ci);
		return newIndex;
	}
	
	public HGIndex<?, HGPersistentHandle> getIndex(HGPersistentHandle typeHandle, String [] dimensionPath)
	{
		HGProjection proj = TypeUtils.getProjection(hg, hg.getTypeSystem().getType(typeHandle), dimensionPath);
		HGAtomType projType = hg.getTypeSystem().getType(proj.getType());
		if (! (projType instanceof ByteArrayConverter && projType instanceof Comparator))
			return null;
/*			throw new HGException("Cannot get index for type " + 
								   typeHandle + 
								   " and dimension path " +
								   TypeUtils.formatDimensionPath(dimensionPath) +
								   " since the type of the projection along that dimension does not implement " +
								   "both the org.hypergraphdb.storage.ByteArrayConverter and java.util.Comparator interfaces."); */				
		else
			return hg.getStore().getIndex(makeIndexName(typeHandle, dimensionPath),  
										 (ByteArrayConverter)projType, 
										  BAtoHandle.getInstance(), 
										  (Comparator)projType);
	}
	
    public void removeAllIndices(HGPersistentHandle typeHandle)
    {
        List<CompositeIndex> l = typeToIndices.get(typeHandle);
        if (l == null)
            return;
        for (CompositeIndex ci : l)
            removeIndex(typeHandle, ci.dimPath);
    }
    
	public void removeIndex(HGPersistentHandle typeHandle, String [] dimensionPath)
	{
		String idx_name = makeIndexName(typeHandle, dimensionPath);
		List l = (List)typeToIndices.get(typeHandle);
		if (l == null)
			throw new HGException("Attempting to remove a non-existing index: " + idx_name);
		String pathAsString = TypeUtils.formatDimensionPath(dimensionPath);
		int pos = 0;
		CompositeIndex idx = null;
		while (pos < l.size())
		{
			idx = (CompositeIndex)l.get(pos);
			if (pathAsString.equals(TypeUtils.formatDimensionPath(idx.dimPath)))
				break;
		}
		if (pos == l.size())
			throw new HGException("Attempting to remove a non-existing index: " + idx_name);
		idx.idx.close();
		l.remove(pos);
		if (l.size() == 0)
			typeToIndices.remove(typeHandle);
		hg.getStore().removeIndex(idx_name);
	}
	
	/**
	 * <p>
	 * Called when an atom is being added to hypergraph to check and possibly
	 * add index entries for the indexed dimensions of the atom's type.
	 * </p>
	 * 
	 * @param typeHandle
	 * @param type
	 * @param atomHandle
	 * @param atom
	 */
	void maybeIndex(HGPersistentHandle typeHandle, 
					HGAtomType type,
				    HGPersistentHandle atomHandle,
				    Object atom)
	{
		List indices = (List)typeToIndices.get(typeHandle);
		if (indices == null)
			return;
		for (Iterator i = indices.iterator(); i.hasNext(); )
		{
			CompositeIndex ci = (CompositeIndex)i.next();
			Object value = atom;
			HGAtomType valueType = type;
			for (int j = 0; j < ci.dimPath.length; j++)
			{
				if (! (valueType instanceof HGCompositeType))
					throw new HGException("Only composite types can be indexed by value parts: " + valueType);
				HGProjection proj = ((HGCompositeType)valueType).getProjection(ci.dimPath[j]);
				if (proj == null)
					throw new HGException("Dimension " + ci.dimPath[j] + 
								" does not exist in type " + hg.getHandle(valueType));
				value = proj.project(value);
				valueType = hg.getTypeSystem().getType(proj.getType());
			}
			ci.idx.addEntry(value, atomHandle);			
		}
	}
	
	/**
	 * <p>
	 * Called when an atom is being added to hypergraph to check and possibly
	 * add index entries for the indexed dimensions of the atom's type.
	 * </p>
	 * 
	 * @param typeHandle
	 * @param type
	 * @param atom
	 * @param atomHandle
	 */
	void maybeUnindex(HGPersistentHandle typeHandle, 
					  HGAtomType type,
	   				  Object atom,
	   				  HGPersistentHandle atomHandle)
	{
		List indices = (List)typeToIndices.get(typeHandle);
		if (indices == null)
			return;
		for (Iterator i = indices.iterator(); i.hasNext(); )
		{
			CompositeIndex ci = (CompositeIndex)i.next();
			Object value = atom;
			HGAtomType valueType = type;
			for (int j = 0; j < ci.dimPath.length; j++)
			{
				if (! (valueType instanceof HGCompositeType))
					throw new HGException("Only composite types can be indexed by value parts: " + valueType);
				HGProjection proj = ((HGCompositeType)valueType).getProjection(ci.dimPath[j]);
				if (proj == null)
					throw new HGException("Dimension " + ci.dimPath[j] + 
								" does not exist in type " + hg.getHandle(valueType));
				value = proj.project(value);
				valueType = hg.getTypeSystem().getType(proj.getType());
			}
			ci.idx.removeEntry(value, atomHandle);			
		}		
	}
}