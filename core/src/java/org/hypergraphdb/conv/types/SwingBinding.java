package org.hypergraphdb.conv.types;

import java.beans.EventSetDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.EventListener;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.JMenuItem;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.conv.DefaultConverter;
import org.hypergraphdb.conv.types.GeneratedClass.SwingRecord;
import org.hypergraphdb.type.HGAtomTypeBase;
import org.hypergraphdb.type.Record;
import org.hypergraphdb.type.Slot;
import org.hypergraphdb.type.TypeUtils;

public class SwingBinding extends HGAtomTypeBase
{
	protected HGHandle typeHandle;
	protected SwingType hgType;
	private SwingTypeIntrospector inspector;
	
    public SwingBinding(){}
    
	public SwingBinding(HGHandle typeHandle, SwingType hgType)
	{
		this.typeHandle = typeHandle;
		this.hgType = hgType;
		hgType.setThisHandle(typeHandle);
	}
	
	protected SwingTypeIntrospector getInspector(){
		if(inspector == null)
			inspector = new SwingTypeIntrospector(graph, hgType);
		return inspector;
	}

	public Object make(HGPersistentHandle handle,
			LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{
		Class<?> c = hgType.getJavaClass();
    	Object bean = null;
		try
		{
			Record record = (Record) hgType.make(handle, targetSet, null);
			//System.out.println("Make: " + c);
			bean = instantiate(hgType.getCtrHandle(), record); // MetaData.getConverter(beanClass).make(map);
			makeBean(bean, record);
			//System.out.println("Make - res: " + bean);
			AddOnLink addons = (AddOnLink) graph.get(hgType.getAddOnsHandle());
			if (addons != null) for (int i = 0; i < addons.getArity(); i++)
			{
				HGRelType l = (HGRelType) graph.get(addons.getTargetAt(i));
				AddOnFactory.processLink(graph, l, record, bean);
			}
			TypeUtils.setValueFor(graph, handle, bean);
		}
		catch (Throwable t)
		{
			throw new HGException("SwingBinding.make: " + t.toString(), t);
		}
		return bean;
	}

	public HGPersistentHandle store(final Object instance)
	{
		Map<Object, HGPersistentHandle> refMap = TypeUtils
				.getTransactionObjectRefMap(graph);
		HGPersistentHandle result = refMap.get(instance);
		if (result == null)
		{
			final Record record = new SwingRecord(typeHandle, instance);
			// System.out.println("Store: " + instance);
			storeBean(instance, record);
			// System.out.println("StoreType: " + hgType + ":"
			// + ((RecordType) hgType).slotCount() + ":" + record);
			// hgType.setHyperGraph(graph);
			// System.out.println("Store1: " + hgType);
			result = hgType.store(record);
		}
		return result;
	}

	public void release(HGPersistentHandle handle)
	{
		hgType.release(handle);
	}

	private Object instantiate(HGHandle h, Record record)
	{
		ConstructorLink link = (ConstructorLink) graph.get(h);
		// System.out.println("SB - instantiate" +
		// hgType.getJavaClass() + ":" + link);
		if (link != null && link instanceof FactoryConstructorLink)
			return AddOnFactory.instantiateFactoryConstructorLink(graph,
					hgType, (FactoryConstructorLink) link, record);
		return AddOnFactory.instantiateConstructorLink(graph, hgType, link,
				record);
	}

	protected void makeBean(Object bean, Record record)
	{
		for (HGHandle slotHandle : hgType.getSlots())
		{
			Slot slot = (Slot) graph.get(slotHandle);
			String label = slot.getLabel();
			Object value = record.get(slot);
			if (hgType.getReferenceMode(slotHandle) != null)
				value = graph.get(((HGAtomRef) value).getReferent());
			// System.out.println("Slot: " + slot.getLabel() + ":" + value);
			if (value == null) continue;
			SwingTypeIntrospector insp = this.getInspector();
			if (insp.getPubFieldsMap().containsKey(label))
				try
				{
					//System.out.println("Field");
					insp.getPubFieldsMap().get(label).set(bean, value);
				}
				catch (IllegalAccessException ex)
				{
					System.err.println("Unable to set field: " + label + " on "
							+ hgType.getJavaClass().getName());
				}
			else if (insp.getEventSetDescriptorsMap().containsKey(label))
			{
				try
				{
					Method m = insp.getEventSetDescriptorsMap().get(label).getAddListenerMethod();
					EventListener[] l = (EventListener[]) value;
					if (l != null) for (EventListener el : l)
						m.invoke(bean, new Object[] { el });
				}
				catch (Throwable t)
				{
					t.printStackTrace();
				}
			} else if(insp.getSettersMap().containsKey(label))
				try
				{
					insp.getSettersMap().get(label).invoke(bean, new Object[]{value});
				}
				catch (Throwable t)
				{
					// System.err.println("Unable to set property: " + label + "
					// on " +
					// hgType.getJavaClass().getName() + ".Reason: " + t);
				}
		}
	}
	
    protected void storeBean(Object bean, Record record){
    	getInspector();
    	try{
    	for (String s : inspector.getGettersMap().keySet())
    	{
    		 setValue(record, s, inspector.getGettersMap().get(s).invoke(bean));
    	}
    	for (Field f : inspector.getPubFieldsMap().values())
    		 setValue(record, f.getName(), f.get(bean));
			
		for (String s : inspector.getEventSetDescriptorsMap().keySet())
		{
			EventSetDescriptor e = inspector.getEventSetDescriptorsMap().get(s);
			if (e != null && !filterListenersByType(e.getListenerType())){
				Method m = e.getGetListenerMethod();
				EventListener[] ls = (EventListener[]) m.invoke(bean);
				setValue(record, e.getName() + DefaultConverter.LISTENERS_KEY, filterListeners(ls));
			}
		}
		for (Field f : inspector.getPrivFieldsMap().values())
		{
			f.setAccessible(true);
			setValue(record, f.getName(), f.get(bean));
		}
		}catch(Exception ex){
			
		}
	}
    
    public void setValue(Record rec, String name, Object val)
    {
    	//System.out.println("SB-setValue:" + hgType.getJavaClass() +
    	//		":" + name + ":" + val);
    	HGHandle slotHandle = hgType.slotHandles.get(name);
    	Slot slot = (Slot)graph.get(slotHandle);
    	HGAtomRef.Mode refMode = hgType.getReferenceMode(slotHandle);
    	if (refMode != null)
    	{
    		HGHandle valueAtomHandle = graph.getHandle(val);
    		if (valueAtomHandle == null)
    			valueAtomHandle = graph.getPersistentHandle(graph.add(val));
    		val = new HGAtomRef(valueAtomHandle, refMode);
    	}
    	rec.set(slot, val);
	}
    
    public Object getValue(Record rec, String name)
	{
		HGHandle slotHandle = hgType.slotHandles.get(name);
		Object value = rec.get((Slot)graph.get(slotHandle));
		if (hgType.getReferenceMode(slotHandle) != null)
    		value = graph.get(((HGAtomRef)value).getReferent());
       return value;
	}
    
    protected boolean filterListenersByType(Class<?> listenerType)
	{
		if (listenerType == java.awt.event.ComponentListener.class)
		{
			return true;
		}
		// JMenuItems have a change listener added to them in
		// their "add" methods to enable accessibility support -
		// see the add method in JMenuItem for details. We cannot
		// instantiate this instance as it is a private inner class
		// and do not need to do this anyway since it will be created
		// and installed by the "add" method. Special case this for now,
		// ignoring all change listeners on JMenuItems.
		if (listenerType == javax.swing.event.ChangeListener.class
				&& hgType.getJavaClass() == javax.swing.JMenuItem.class)
		{
			return true;
		}
		return false;
	}
    
    protected EventListener[] filterListeners(EventListener[] in) {
		if(in == null) return null;
		Set<EventListener> res = new HashSet<EventListener>(in.length);
		for (EventListener e : in) {
			if (e.getClass().getName().startsWith("javax.swing.plaf.")) {
				//System.err.println("Filtering " + e);
				continue;
			}
			if (e.getClass().isMemberClass()
					&& !Modifier.isStatic(e.getClass().getModifiers())) {
				//System.err.println("Filtering " + e);
				continue;
			}
			// normally those listeners will be added during construction
			if (e.getClass().equals(hgType.getJavaClass())) {
				//System.err.println("Filtering " + e);
				continue;
			}
			if (e.getClass().getEnclosingClass() == JMenuItem.class
					&& e.getClass().getName().startsWith(
							"javax.swing.JMenuItem$")) {
				System.err.println("Filtering " + e);
				continue;
			}
			res.add(e);
		}
		return res.toArray(new EventListener[res.size()]);
	}
}