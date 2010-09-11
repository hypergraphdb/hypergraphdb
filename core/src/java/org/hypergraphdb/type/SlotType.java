/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.util.Iterator;
import java.util.HashMap;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;

/**
 * <p>
 * The <code>SlotType</code> handles atoms of type <code>Slot</code>. Slots are used to
 * build up record types. 
 * </p>
 * 
 * <p>
 * <strong>IMPLEMENTATION NOTE:</strong> Eventually, it will be useful to be able to
 * search by slot name or slot value type only. To support this, it's probably best
 * to create a separate index for each component. Also, index management should 
 * probably be updated once we have more flexible two-way indexes that represent
 * a 1-1 mapping (so that a key may be efficiently searched by the value of the index
 * entry); right now, removal of a slot is implemented by first constructing the Slot
 * instance...(which is ok as long as this removal is not performed very often).
 * </p>
 * @author Borislav Iordanov
 */
public final class SlotType implements HGCompositeType, HGSearchable<Slot, HGPersistentHandle> 
{
    public static final String INDEX_NAME = "hg_slot_value_index";
    public static final String LABEL_DIMENSION = "label";
    public static final String VALUE_TYPE_DIMENSION = "valueType";
    
    private HyperGraph hg;
    private HGIndex<byte[], HGPersistentHandle> slotIndex = null;
    
    private byte [] slotToByteArray(Slot slot)
    {
        HGPersistentHandle valueType = hg.getPersistentHandle(slot.getValueType());
        byte [] valueTypeAsBytes = valueType.toByteArray();
        byte [] labelAsBytes = slot.getLabel().getBytes();
        
        byte [] result = new byte[valueTypeAsBytes.length + labelAsBytes.length];
        System.arraycopy(valueTypeAsBytes, 0, result, 0, valueTypeAsBytes.length);
        System.arraycopy(labelAsBytes, 0, result, valueTypeAsBytes.length, labelAsBytes.length);
        return result;
    }
    
    private HGIndex<byte[], HGPersistentHandle> getIndex()
    {
        if (slotIndex == null)
        {
            slotIndex = hg.getStore().getIndex(INDEX_NAME, 
                                               BAtoBA.getInstance(), 
                                               BAtoHandle.getInstance(hg.getHandleFactory()), 
                                               null,
                                               true);
        }
        return slotIndex;
    }
    
    public void setHyperGraph(HyperGraph hg)
    {
        this.hg = hg;
    }
    
    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {
        HGPersistentHandle [] layout = hg.getStore().getLink(handle);
        HGAtomType labelType = hg.getTypeSystem().getAtomType(String.class);
        String label = (String)labelType.make(layout[1], null, null);        
        return new Slot(label, layout[0]);
    }

    public HGPersistentHandle store(Object instance)
    {       
        Slot slot = (Slot)instance;
        HGTypeSystem typeSystem = hg.getTypeSystem();        
        HGPersistentHandle valueTypeHandle = hg.getPersistentHandle(slot.getValueType());
        if (getIndex().findFirst(slotToByteArray(slot)) != null)
            throw new HGException("Attempting to create a duplicate slot: [name=" +
                        slot.getLabel() + ", value type=" + valueTypeHandle);
        else
        {   
            HGAtomType stringType = typeSystem.getAtomType(String.class);
            HGPersistentHandle labelHandle = stringType.store(slot.getLabel());
            HGPersistentHandle handle = hg.getStore().store( 
                    new HGPersistentHandle[]
                                { hg.getPersistentHandle(slot.getValueType()),
                                  labelHandle,});
            getIndex().addEntry(slotToByteArray(slot), handle);
            return handle;     
        }
    }

    public void release(HGPersistentHandle handle)
    {
        Object slot = make(handle, null, null);
        getIndex().removeAllEntries(slotToByteArray((Slot)slot));
        hg.getStore().removeLink(handle);
    }

    public boolean subsumes(Object general, Object specific)
    {
        return false;
    }

    public HGSearchResult<HGPersistentHandle> find(Slot key)
    {
        byte [] asByteArray = slotToByteArray(key);
        return getIndex().find(asByteArray);
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
		return projections.get(dimensionName);
	}
    
    private HashMap<String, HGProjection> projections = null;
    	
    private synchronized void initProjections()
    {
    	if (projections != null)
    		return;
    	
        projections = new HashMap<String, HGProjection>();
        projections.put(LABEL_DIMENSION, new LabelProjection());
        projections.put(VALUE_TYPE_DIMENSION, new ValueTypeProjection());
    }
    
    private final class LabelProjection implements HGProjection
    {
    	private final int [] layoutPath = new int[] {1};
    	
    	public String getName() { return SlotType.LABEL_DIMENSION; }
    	public HGHandle getType() { return hg.getTypeSystem().getTypeHandle(String.class); }
    	public Object project(Object value) { return ((Slot)value).getLabel(); }
    	public void inject(Object slot, Object label) { ((Slot)slot).setLabel((String)label); }
    	public int [] getLayoutPath() { return layoutPath; } 
    };
    
    private final class ValueTypeProjection implements HGProjection
    {
    	private int [] layoutPath = new int[] {0};
    	
    	public String getName() { return SlotType.VALUE_TYPE_DIMENSION; }
    	public HGHandle getType() { return hg.getTypeSystem().getTypeHandle(Top.class); }    	
    	public Object project(Object value) { return ((Slot)value).getValueType(); }
    	public void inject(Object slot, Object valueType) { ((Slot)slot).setValueType((HGHandle)valueType); }    	
    	public int [] getLayoutPath() { return layoutPath; } 
    };
}
