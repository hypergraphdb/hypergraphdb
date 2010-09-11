/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.lang.reflect.Constructor;
import java.util.Stack;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.util.Pair;

public class JavaObjectBinding extends JavaAbstractBinding
{
    private Constructor<?> linkConstructor = null;
    
    public JavaObjectBinding(HGHandle typeHandle, RecordType hgType, Class<?> clazz)
    {
    	super(typeHandle, hgType, clazz);
        try
        {
        	linkConstructor = javaClass.getDeclaredConstructor(new Class[] {HGHandle[].class} );
        }
        catch (NoSuchMethodException ex) { }
    }

    private void assignFields(HGPersistentHandle valueHandle, Object instance)
    {    	 
    	HGHandle superSlot = JavaTypeFactory.getSuperSlot(graph);
    	RecordType hgType = (RecordType)this.hgType;
    	Class<?> clazz = javaClass;    	
    	while (true)
    	{
    		Record record = (Record)hgType.make(valueHandle, null, null);
    		HGPersistentHandle ss = null;
	        for (HGHandle slotHandle : hgType.getSlots())
	        {	 
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	if (slotHandle.equals(superSlot))
	        	{
	        		ss = (HGPersistentHandle)record.get(slot);
	        		continue;
	        	}	        	
	        	Object value = record.get(slot);
	        	if (value != null && hgType.getReferenceMode(slotHandle) != null)
	        		value = graph.get(((HGAtomRef)value).getReferent());
	            JavaTypeFactory.assignPrivate(clazz, instance, slot.getLabel(), value);	        	
	        }
	        if (ss != null)
        	{	
	        	clazz = clazz.getSuperclass();
        		JavaAbstractBinding superType = (JavaAbstractBinding)graph.getTypeSystem().getAtomType(clazz);	        	
        		hgType = (RecordType)superType.getHGType();
        		valueHandle = ss;
        	}
	        else
	        	break;
    	}
    }
    
    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {
        Object result = null;
        try
        {            
            if (targetSet != null && targetSet.deref().length > 0)
            	if (linkConstructor != null)
            		result = linkConstructor.newInstance(new Object[] { targetSet.deref() });
            	else
            		throw new RuntimeException("Can't construct link with Java type " +
            				javaClass.getName() + " please include a (HGHandle [] ) constructor.");
            else
            	result = javaClass.newInstance();
            TypeUtils.setValueFor(graph, handle, result);            
	        assignFields(handle, result);
        }
        catch (InstantiationException ex)
		{
        	throw new HGException("Unable to instantiate bean of type '" + javaClass.getName() +
        			"', make sure that bean has a default constructor declared.");
		}
        catch (Throwable t)
        {
            throw new HGException("JavaTypeBinding.make: " + t.toString(), t);
        }
        return result;
    }

    public HGPersistentHandle store(final Object instance)
    {     	
		HGPersistentHandle result = TypeUtils.getHandleFor(graph, instance);
		if (result == null)
		{
            HGHandle superSlotHandle = JavaTypeFactory.getSuperSlot(graph);
            Slot superSlot = graph.get(superSlotHandle);
            Class<?> clazz = javaClass;
	        RecordType recordType = (RecordType)hgType;
	        Record record = new BeanRecord(typeHandle, instance);
	        Stack<Pair<RecordType, Record>> superList = new Stack<Pair<RecordType, Record>>();
	        while (true)
	        {		        
		        superList.push(new Pair<RecordType, Record>(recordType, record));
		        boolean has_super = false;
		        for (HGHandle slotHandle : recordType.getSlots())
		        {        	
		        	if (slotHandle.equals(superSlotHandle))
		        	{
		        		has_super = true;
		        		continue;
		        	}
		        	else
		        	{
		        		Slot slot = (Slot)graph.get(slotHandle);
			        	// Normal field declared at the level of instances' class.
			        	Object value = JavaTypeFactory.retrievePrivate(clazz, instance, slot.getLabel());
			        	HGAtomRef.Mode refMode = recordType.getReferenceMode(slotHandle);
			        	if (refMode != null && value != null)
			        	{
			        		HGHandle valueAtomHandle = graph.getHandle(value);
			        		if (valueAtomHandle == null)
			        		{
			        			HGAtomType valueType = (HGAtomType)graph.get(slot.getValueType());
			        			valueAtomHandle = graph.getPersistentHandle(
			        					graph.add(value, 
			        						valueType instanceof HGAbstractType ?
			        							 graph.getTypeSystem().getTypeHandle(value.getClass()) :	
			        							 slot.getValueType()));
			        		}
			        		value = new HGAtomRef(valueAtomHandle, refMode);
			        	}
		        		record.set(slot, value);
		        	}
		        }
		        if (has_super)
		        {
		        	clazz = clazz.getSuperclass();
		        	HGHandle superTypeHandle = graph.getTypeSystem().getTypeHandle(clazz);
		        	JavaAbstractBinding superType = graph.get(superTypeHandle); 
		        	recordType = (RecordType)superType.getHGType();		
		        	record = new Record(superTypeHandle);
		        }
		        else 
		        	break;
	        }	        
	        while (!superList.isEmpty())
	        {
	        	Pair<RecordType, Record> curr = superList.pop();
	        	if (result != null)
	        		curr.getSecond().set(superSlot, result);
	        	result = curr.getFirst().store(curr.getSecond());
	        }
		}
		return result;
    }

    public void release(HGPersistentHandle handle)
    {
    	Record rec = (Record)hgType.make(handle, null, null);
    	HGPersistentHandle parent = (HGPersistentHandle)
    		rec.get(new Slot("!super", graph.getTypeSystem().getTypeHandle(HGPersistentHandle.class)));
    	if (parent != null)
    	{
    		HGAtomType superType = graph.getTypeSystem().getAtomType(javaClass.getSuperclass());
    		superType.release(parent);
    	}
    	hgType.release(handle);
    }
    
    static class BeanRecord extends Record implements TypeUtils.WrappedRuntimeInstance
    {
    	Object bean;    	
    	BeanRecord(HGHandle h, Object bean) { super(h); this.bean = bean;}
    	public Object getRealInstance() { return bean; }
    }
}
