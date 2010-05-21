/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.lang.reflect.Constructor;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.atom.HGAtomRef;

/**
 * <p>
 * Acts as a <code>HGAtomType</code> for Java beans. Uses underlying
 * <code>RecordType</code> instance to manage the layout in the store, but
 * operates on actual bean instances instead of <code>Record</code>s.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class JavaBeanBinding extends JavaAbstractBinding
{
    private Constructor<?> linkConstructor = null;

        
    public JavaBeanBinding(HGHandle typeHandle, RecordType hgType, Class<?> clazz)
    {
    	super(typeHandle, hgType, clazz);
        try
        {
        	linkConstructor = javaClass.getDeclaredConstructor(new Class[] {HGHandle[].class} );
        }
        catch (NoSuchMethodException ex) { }
    }

    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
    {
        Object bean = null;
        try
        {
            JavaTypeFactory javaTypes = graph.getTypeSystem().getJavaTypeFactory();
            if (linkConstructor != null)
            {
            	if (targetSet != null)
            		bean = linkConstructor.newInstance(new Object[] { targetSet.deref() });
            	else
            		bean = javaClass.newInstance();
            }
            else if (targetSet != null && targetSet.deref().length > 0)
        		throw new RuntimeException("Can't construct link with Java type " +
        				javaClass.getName() + " please include a (HGHandle [] ) constructor.");
            else
           	   bean = javaClass.newInstance();
            TypeUtils.setValueFor(graph, handle, bean);            
            Record record = (Record)hgType.make(handle, targetSet, null);
	        RecordType recordType = (RecordType)hgType;
	        for (HGHandle slotHandle : recordType.getSlots())
	        {
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	Object value = record.get(slot);
	        	if (value != null && recordType.getReferenceMode(slotHandle) != null)
	        		value = graph.get(((HGAtomRef)value).getReferent());
	        	try
	        	{
	        		javaTypes.assign(bean, slot.getLabel(), value);
	        	}
	        	catch (Throwable t)
	        	{	        		
	        		throw new HGException("Failed to assign property: " + 
	        							  slot.getLabel() + 
	        							  " to bean " + 
	        							  bean.getClass(), t);
	        	}
	        }
        }
        catch (InstantiationException ex)
		{
        	throw new HGException("Unable to instantiate bean of type '" + javaClass.getName() +
        			"', make sure that bean has a default constructor declared.", ex);
		}
        catch (HGException t) { throw t; }
        catch (Throwable t)
        {
            throw new HGException("JavaTypeBinding.make: " + t.toString(), t);
        }
        return bean;
    }

    public HGPersistentHandle store(final Object instance)
    {    	
		HGPersistentHandle result = TypeUtils.getHandleFor(graph, instance);
		if (result == null)
		{
	        final Record record = new BeanRecord(typeHandle, instance);
	        RecordType recordType = (RecordType)hgType;
	        for (HGHandle slotHandle : recordType.getSlots())
	        {
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	Object value = BonesOfBeans.getProperty(instance, slot.getLabel());
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
        	result = hgType.store(record);
		}
		return result;
    }

    public void release(HGPersistentHandle handle)
    {
    	hgType.release(handle);
    }
    
    static class BeanRecord extends Record implements TypeUtils.WrappedRuntimeInstance
    {
    	Object bean;    	
    	BeanRecord(HGHandle h, Object bean) { super(h); this.bean = bean;}
    	public Object getRealInstance() { return bean; }
    }
}
