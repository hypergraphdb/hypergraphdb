/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.util.Map;
import java.lang.reflect.Constructor;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
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
public class JavaBeanBinding extends JavaAbstractBeanBinding
{
    private Constructor linkConstructor = null;

        
    public JavaBeanBinding(HGHandle typeHandle, RecordType hgType, Class clazz)
    {
    	super(typeHandle, hgType, clazz);
        try
        {
        	linkConstructor = beanClass.getDeclaredConstructor(new Class[] {HGHandle[].class} );
        }
        catch (NoSuchMethodException ex) { }
    	hgType.setThisHandle(typeHandle);
    }

    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet)
    {
        Object bean = null;
        try
        {
            JavaTypeFactory javaTypes = JavaTypeFactory.getInstance();
            if (targetSet != null && targetSet.deref().length > 0)
            	if (linkConstructor != null)
            		bean = linkConstructor.newInstance(new Object[] { targetSet.deref() });
            	else
            		throw new RuntimeException("Can't construct link with Java type " +
            				beanClass.getName() + " please include a (HGHandle [] ) constructor.");
            else
           	   bean = beanClass.newInstance();
            TypeUtils.setValueFor(graph, handle, bean);            
            Record record = (Record)hgType.make(handle, targetSet, null);
	        RecordType recordType = (RecordType)hgType;
	        for (HGHandle slotHandle : recordType.getSlots())
	        {
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	Object value = record.get(slot);
	        	if (recordType.getReferenceMode(slotHandle) != null)
	        		value = graph.get(((HGAtomRef)value).getReferent());
                javaTypes.assign(bean, slot.getLabel(), value);	        	
	        }
        }
        catch (InstantiationException ex)
		{
        	throw new HGException("Unable to instantiate bean of type '" + beanClass.getName() +
        			"', make sure that bean has a default constructor declared.");
		}
        catch (Throwable t)
        {
            throw new HGException("JavaTypeBinding.make: " + t.toString(), t);
        }
        return bean;
    }

    public HGPersistentHandle store(final Object instance)
    {
		Map<Object, HGPersistentHandle> refMap = TypeUtils.getTransactionObjectRefMap(graph);    	
		HGPersistentHandle result = refMap.get(instance);
		if (result == null)
		{
	        final Record record = new BeanRecord(typeHandle, instance);
	        RecordType recordType = (RecordType)hgType;
	        for (HGHandle slotHandle : recordType.getSlots())
	        {
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	Object value = BonesOfBeans.getProperty(instance, slot.getLabel());
	        	HGAtomRef.Mode refMode = recordType.getReferenceMode(slotHandle);
	        	if (refMode != null)
	        	{
	        		HGHandle valueAtomHandle = graph.getHandle(value);
	        		if (valueAtomHandle == null)
	        			valueAtomHandle = graph.getPersistentHandle(graph.add(value));
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