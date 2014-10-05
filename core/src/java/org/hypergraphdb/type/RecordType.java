/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.HashCodeUtil;

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
 * <p>
 * A null record is represented with a null handle {@link HGHandleFactory.nullHandle()}, while
 * and empty record is represented with a newly created handle that doesn't point to anything. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class RecordType implements HGCompositeType
{
    protected ArrayList<HGHandle> slots = new ArrayList<HGHandle>();
    protected HyperGraph graph;
    protected HGHandle thisHandle;
    private HashMap<String, HGProjection> projections = null;
    private HashMap<HGHandle, HGAtomRef.Mode> refModes = null;
            
    private synchronized void initProjections()
    {
    	if (projections != null)
    		return;
    	HashMap<String, HGProjection> tmp = new HashMap<String, HGProjection>();
    	for (int i = 0; i < slots.size(); i++)
    	{
    		Slot slot = (Slot)graph.get(slots.get(i));
    		tmp.put(slot.getLabel(), new SlotBasedProjection(slot, new int [] {i}));
    	}
    	projections = tmp;
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
	    		if (thisHandle == null)
	    			thisHandle = graph.getHandle(this);
	    		List<AtomProjection> L = hg.getAll(graph, hg.and(hg.type(AtomProjection.class), 
	    							   hg.incident(thisHandle),
	    							   hg.orderedLink(thisHandle, graph.getHandleFactory().anyHandle())));
	    		for (AtomProjection l : L)
	    		{
	    			HGHandle slotHandle = hg.findOne(graph, hg.eq(new Slot(l.getName(),l.getProjectionValueType())));	    			
	    			refModes.put(slotHandle, l.getMode());
	    		}
	    	}
	    	catch (RuntimeException ex)
	    	{
	    		refModes = null;
	    		//return null;
	    		throw ex;
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
   
    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {
    	if (graph.getHandleFactory().nullHandle().equals(handle))
    		return null;
        Record result = null;
        HGHandle [] targets = HGUtils.EMPTY_HANDLE_ARRAY;
        if (targetSet != null)
        {
            targets = targetSet.deref();
            if (targets == null)
                targets = HGUtils.EMPTY_HANDLE_ARRAY;
        }
        if (targets.length > 0)
            result = new LinkRecord(graph.getHandle(this), targets);
        else
            result = new Record(graph.getHandle(this));
        TypeUtils.setValueFor(graph, handle, result);
        HGPersistentHandle [] layout = slots.isEmpty() ? 
                                        HGUtils.EMPTY_HANDLE_ARRAY :
                                        graph.getStore().getLink(handle);
        if (layout.length != slots.size() * 2)
            throw new HGException("RecordType.make: Record value of handle " + 
                                  handle + 
                                  " does not match record type number of slots.");
        for (int i = 0; i < slots.size(); i++)
        {
        	HGHandle slotHandle = getAt(i);             
            Object value = null;
//            try
//            {
            if (!layout[2*i + 1].equals(graph.getHandleFactory().nullHandle()))
            {            	
	        	HGAtomRef.Mode refMode = getReferenceMode(slotHandle);
	        	if (refMode != null)
	        	{
	        		AtomRefType refType = graph.getTypeSystem().getAtomType(HGAtomRef.class);
	        		value = refType.make(layout[2*i + 1], null, null);
	        	}
	        	else
	        		value = TypeUtils.makeValue(graph, 
	        									layout[2*i + 1], 
	        									graph.getTypeSystem().getType(layout[2*i]));
            }
//            }
//            catch (HGException ex)
//            {
//            	Slot s = graph.get(slotHandle);
//            	System.err.println("Unable to get value for slot: " + s.getLabel());
//            	throw ex;
//            }
            result.set((Slot)graph.get(slotHandle), value);
        }
        return result;
    }

    public HGPersistentHandle store(Object instance)
    {
    	if (instance == null)
    		return graph.getHandleFactory().nullHandle();
    	HGPersistentHandle handle = TypeUtils.getNewHandleFor(graph, instance);    	
    	if (slots.isEmpty())
            return handle;        
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
            	layout[2*i + 1] = graph.getHandleFactory().nullHandle();
            }
            else
            {
	        	HGAtomRef.Mode refMode = getReferenceMode(slotHandle);        	
	        	if (refMode == null)
	        	{
	                HGHandle actualTypeHandle = graph.getTypeSystem().getTypeHandle(value.getClass());
	                if (actualTypeHandle == null)
	                	actualTypeHandle = slot.getValueType();
	                else if (actualTypeHandle.equals(graph.getTypeSystem().getTop()))
	                	throw new HGException("Got TOP type for value for Java class " + value.getClass());
	                HGAtomType type = graph.getTypeSystem().getType(actualTypeHandle);                
	                layout[2*i] = graph.getPersistentHandle(actualTypeHandle);
	                try
	                {
	                	layout[2*i + 1] = TypeUtils.storeValue(graph, value, type);
	                }
	                catch (HGException ex)
	                {
	                	throw new HGException("Failed on slot '" + slot.getLabel() + "' of class " + value.getClass(),ex);
	                }
	        	}
	        	else
	        	{
	                layout[2*i] = graph.getPersistentHandle(slot.getValueType());
	                if (value instanceof HGAtomRef)
	                {
		        		AtomRefType refType = graph.getTypeSystem().getAtomType(HGAtomRef.class);
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
        if (slots.isEmpty() || graph.getHandleFactory().nullHandle().equals(handle))
            return;
        HGPersistentHandle [] layout = graph.getStore().getLink(handle);
        if (layout == null)
            // this is fishy, a sys print out like this, next line will throw an NPE anyway
        	System.out.println("oops, no data for : " + handle); 
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
            	type = graph.getTypeSystem().getAtomType(HGAtomRef.class);
            int j = 2*i + 1;
            if (!layout[j].equals(graph.getHandleFactory().nullHandle()))
            	if (!TypeUtils.isValueReleased(graph, layout[j]))
            	{
            		TypeUtils.releaseValue(graph, type, layout[j]);
            	}
        }        
        graph.getStore().removeLink(handle);
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
    
    public int hashCode()
    {
    	int hash = HGUtils.hashIt(this.thisHandle); 
    	for (HGHandle sh : slots)
    		hash = HashCodeUtil.hash(hash, sh);
    	return hash;
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
