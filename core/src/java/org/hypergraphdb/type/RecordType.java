/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.util.HGUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * <p>
 * A <code>RecordType</code> represents a particular combination of slots that
 * can be used to construct records. The latter are instances of <code>Record</code>.
 * </p>
 * 
 * <p>
 * It is important that the slots in a record type are maintained in an order matching the HG store layout
 * of record values.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class RecordType implements HGCompositeType
{
    private ArrayList<HGHandle> slots = new ArrayList<HGHandle>();
    private HyperGraph graph;
    private HGHandle thisHandle;
    private HashMap<String, HGProjection> projections = null;
    private HashMap<HGHandle, HGAtomRef.Mode> refModes = null;
            
    private synchronized void initProjections()
    {
    	if (projections != null)
    		return;
    	projections = new HashMap<String, HGProjection>();
    	for (int i = 0; i < slots.size(); i++)
    	{
    		Slot slot = (Slot)graph.get(slots.get(i));
    		projections.put(slot.getLabel(), new SlotBasedProjection(slot, new int [] {i}));
    	}
    }
    
    public RecordType()
    {    
    }
    
    public synchronized HGAtomRef.Mode getReferenceMode(HGHandle slot)
    {
    	if (refModes == null)
    	{
    		refModes = new HashMap<HGHandle, HGAtomRef.Mode>();    	
	    	HGSearchResult<HGHandle> rs = null;
	    	try
	    	{
	    		rs = graph.find(hg.and(hg.type(AtomProjection.class), 
	    							   hg.link(thisHandle == null ? graph.getHandle(this) : thisHandle)));
	    		while (rs.hasNext())
	    		{
	    			AtomProjection l = (AtomProjection)graph.get(rs.next());
	    			refModes.put(l.getProjection(), l.getMode());
	    		}
	    	}
	    	finally
	    	{
	    		HGUtils.closeNoException(rs);
	    	}
    	}
    	return refModes.get(slot);
    }
    
    public void setThisHandle(HGHandle thisHandle)
    {
    	this.thisHandle = thisHandle;
    }
    
	public Iterator<String> getDimensionNames()
	{
		if (projections == null)
			initProjections();
		return projections.keySet().iterator();
	}
	
	public HGProjection getProjection(String dimensionName)
	{
		if (projections == null)
			initProjections();
		return (HGProjection)projections.get(dimensionName);
	}
    
	public List<HGHandle> getSlots()
	{
		return slots; 
	}
	
    public void addSlot(HGHandle slot)
    {
        if (!slots.contains(slot))
            slots.add(slot);
    }
    
    public void remove(HGHandle slot)
    {
        slots.remove(slot);
    }
    
    public void removeAt(int i)
    {
        slots.remove(i);
    }
    
    public HGHandle getAt(int i)
    {
        return slots.get(i);
    }
    
    public int slotCount()
    {
        return slots.size();
    }
    
    public void setHyperGraph(HyperGraph hg)
    {
        this.graph = hg;
    }
   
    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet)
    {
        Record result = null;
        if (targetSet != null && targetSet.deref().length > 0)
            result = new LinkRecord(graph.getHandle(this), targetSet.deref());
        else
            result = new Record(graph.getHandle(this));
        TypeUtils.setValueFor(graph, handle, result);
        HGPersistentHandle [] layout = graph.getStore().getLink(handle);
        if (layout.length != slots.size() * 2)
            throw new HGException("RecordType.make: Record value of handle " + 
                                  handle + 
                                  " does not match record type number of slots.");
        for (int i = 0; i < slots.size(); i++)
        {
        	HGHandle slotHandle = getAt(i);             
            Object value = null;
            if (!layout[2*i + 1].equals(HGHandleFactory.nullHandle()))
            {            	
	        	HGAtomRef.Mode refMode = getReferenceMode(slotHandle);
	        	if (refMode != null)
	        	{
	        		AtomRefType refType = (AtomRefType)graph.get(AtomRefType.HGHANDLE);
	        		value = refType.make(layout[2*i + 1], null, null);
	        	}
	        	else
	        		value = TypeUtils.makeValue(graph, 
	        									layout[2*i + 1], 
	        									graph.getTypeSystem().getType(layout[2*i]));
            }            
            result.set((Slot)graph.get(slotHandle), value);
        }
        return result;
    }

    public HGPersistentHandle store(Object instance)
    {
        HGPersistentHandle handle = TypeUtils.getNewHandleFor(graph, instance);
        if (! (instance instanceof Record))
            throw new HGException("RecordType.store: object is not of type Record.");
        Record record = (Record)instance;
        HGPersistentHandle [] layout = new HGPersistentHandle[slots.size() * 2];
        for (int i = 0; i < slots.size(); i++)
        {        	
        	HGHandle slotHandle = getAt(i);
            Slot slot = (Slot)graph.get(slotHandle);
            Object value = record.get(slot);            
            if (value == null)
            {
            	layout[2*i] = graph.getPersistentHandle(slot.getValueType());
                layout[2*i + 1] = HGHandleFactory.nullHandle();            	
                continue;
            }
            
            if (value == null)
            {
            	layout[2*i] = graph.getPersistentHandle(slot.getValueType());
            	layout[2*i + 1] = HGHandleFactory.nullHandle();
            }
            else
            {
	        	HGAtomRef.Mode refMode = getReferenceMode(slotHandle);        	
	        	if (refMode == null)
	        	{
	                HGHandle actualTypeHandle = graph.getTypeSystem().getTypeHandle(value.getClass());
	                if (actualTypeHandle == null)
	                	actualTypeHandle = slot.getValueType();
	                HGAtomType type = graph.getTypeSystem().getType(actualTypeHandle);                
	                layout[2*i] = graph.getPersistentHandle(actualTypeHandle);
	                layout[2*i + 1] = TypeUtils.storeValue(graph, value, type);
	        	}
	        	else
	        	{
	                layout[2*i] = graph.getPersistentHandle(slot.getValueType());
	                if (value instanceof HGAtomRef)
	                {
		        		AtomRefType refType = (AtomRefType)graph.get(AtomRefType.HGHANDLE);
		                layout[2*i + 1] = refType.store((HGAtomRef)value);
	                }
	                else
	                	throw new HGException("Slot " + slot.getLabel() + 
	                						  " should have an atom reference for record " + 
	                						  graph.getHandle(this));
	        	}
            }
        }
        graph.getStore().store(handle, layout);
        return handle;
    }
    
    public void release(HGPersistentHandle handle)
    {    	
        HGPersistentHandle [] layout = graph.getStore().getLink(handle);
        if (layout.length != slots.size() * 2)
            throw new HGException("RecordType.remove: Record value of handle " + 
                                  handle + 
                                  " does not match record type number of slots.");       
        for (int i = 0; i < slots.size(); i++)
        {
        	HGHandle slotHandle = (HGHandle)getAt(i);
            HGAtomType type;
            if (getReferenceMode(slotHandle) == null)
            	type = graph.getTypeSystem().getType(layout[2*i]);
            else
            	type = (HGAtomType)graph.get(AtomRefType.HGHANDLE);
            int j = 2*i + 1;
            if (!layout[j].equals(HGHandleFactory.nullHandle()))
            	if (!TypeUtils.isValueReleased(graph, layout[j]))
            	{
            		TypeUtils.releaseValue(graph, layout[j]);
            		type.release(layout[j]);
            	}
        }        
        graph.getStore().remove(handle);
    }

    public boolean subsumes(Object general, Object specific)
    {
        return false;
    }
    
    public boolean equals(Object other)
    {
        if (! (other instanceof RecordType))
            return false;
        else
        {
            RecordType otherR = (RecordType)other;
            if(slots.size() != otherR.slots.size())
                return false;
            for(int i =0; i < slots.size(); i++)
                if(!slots.get(i).equals(otherR.slots.get(i)))
                    return false;
            return true;
        }
    }
    
    public String toString()
    {
        String res = "";
        for(int i =0; i < slots.size(); i++)
        {
            String n = ((Slot)graph.get(getAt(i))).getValueType().getClass().getName();            
            res += n.substring(n.lastIndexOf('.') + 1);
            if(i != slots.size() -1)
              res += "/";
        }
        return res;
    }
}