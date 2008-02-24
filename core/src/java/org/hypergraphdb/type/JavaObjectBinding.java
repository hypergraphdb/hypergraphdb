package org.hypergraphdb.type;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.atom.HGAtomRef;

public class JavaObjectBinding extends JavaAbstractBeanBinding
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
    	hgType.setThisHandle(typeHandle);
    }

    private void assignFields(HGPersistentHandle valueHandle, Object instance)
    {    	
    	JavaTypeFactory javaTypes = graph.getTypeSystem().getJavaTypeFactory();
        Record record = (Record)hgType.make(valueHandle, null, null);
        RecordType recordType = (RecordType)hgType;    	
        for (HGHandle slotHandle : recordType.getSlots())
        {
        	Slot slot = (Slot)graph.get(slotHandle);
        	if ("!super".equals(slot.getLabel()))
        	{
        		JavaObjectBinding superType = (JavaObjectBinding)graph.getTypeSystem().getAtomType(javaClass.getSuperclass());
        		superType.assignFields((HGPersistentHandle)record.get(slot), instance);
        	}
        	Object value = record.get(slot);
        	if (value != null && recordType.getReferenceMode(slotHandle) != null)
        		value = graph.get(((HGAtomRef)value).getReferent());
            javaTypes.assignPrivate(javaClass, instance, slot.getLabel(), value);	        	
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
		Map<Object, HGPersistentHandle> refMap = TypeUtils.getTransactionObjectRefMap(graph);    	
		HGPersistentHandle result = refMap.get(instance);
		if (result == null)
		{
            JavaTypeFactory javaTypes = graph.getTypeSystem().getJavaTypeFactory();
	        final Record record = new BeanRecord(typeHandle, instance);
	        RecordType recordType = (RecordType)hgType;
	        for (HGHandle slotHandle : recordType.getSlots())
	        {
	        	Slot slot = (Slot)graph.get(slotHandle);
	        	if ("!super".equals(slot.getLabel()))
	        	{
	        		HGAtomType superType = graph.getTypeSystem().getAtomType(javaClass.getSuperclass());
	        		record.set(slot, superType.store(instance));	        		
	        	}
	        	else
	        	{
		        	// Normal field declared at the level of instances' class.
		        	Object value = javaTypes.retrievePrivate(javaClass, instance, slot.getLabel());
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
        	result = hgType.store(record);
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
