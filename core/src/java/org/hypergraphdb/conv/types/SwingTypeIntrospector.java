package org.hypergraphdb.conv.types;

import java.beans.BeanInfo;
import java.beans.EventSetDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.conv.DefaultConverter;
import org.hypergraphdb.conv.GenUtils;
import org.hypergraphdb.conv.RefUtils;
import org.hypergraphdb.type.Slot;

public class SwingTypeIntrospector
{
	protected SwingType type;
	protected HyperGraph hg;
	
	protected Map<String, Field> pub_fields = new HashMap<String, Field>();
	protected Map<String, Field> priv_fields = new HashMap<String, Field>();
	protected Map<String, Method> setters = new HashMap<String, Method>();
	protected Map<String, Method> getters = new HashMap<String, Method>();
	protected Map<String, EventSetDescriptor> esd = new HashMap<String, EventSetDescriptor>();
	protected Map<String, Class> ctrs;
	protected Map<String, Class> addons;

	public SwingTypeIntrospector(HyperGraph hg, SwingType type)
	{
		this.type = type;
		this.hg = hg;
		//System.out.println("SwingTypeIntrospector: " + type.getJavaClass());
		introspectSlots();
	}
	
	private void introspectSlots()
	{
		ctrs = GenUtils.getCtrSlots(hg, type);
		addons = AddOnFactory.getAddOnSlots(hg, type);
		Class javaClass = type.getJavaClass();
		BeanInfo info = RefUtils.getBeanInfo(javaClass);
		for (EventSetDescriptor e : info.getEventSetDescriptors())
			esd.put(e.getName() + DefaultConverter.LISTENERS_KEY, e);
		for (HGHandle slotHandle : type.getSlots())
		{
			Slot slot = (Slot) hg.get(slotHandle);
			if(slot == null) continue;
			String label = slot.getLabel();
			//System.out.println("Slot: " + slot.getLabel());
			Field f = RefUtils.getPublicField(javaClass, label);
			if (f != null)
			{
				pub_fields.put(slot.getLabel(), f);
				continue;
			} else if (label.endsWith(DefaultConverter.LISTENERS_KEY))
			{
				;// esd.put(label, esd.get(label));
			} else if (!pub_fields.containsKey(label))
			{
				Method m = RefUtils.getGetMethod(javaClass, label);
				Method m1 = RefUtils.getSetMethod(javaClass, label);
				if (m != null && m1 != null
						&& checkType(label, m.getReturnType()))
				{
					getters.put(label, m);
					if (needSetter(label)) setters.put(label, m1);
				} else
				{
					f = RefUtils.getPrivateField(javaClass, label);
					if (f != null)
						priv_fields.put(label, f);
					else if (m != null)
					{
						getters.put(label, m);
					}
				}
			}
		}
	}

	private boolean checkType(String label, Class t)
	{
		if (ctrs != null && ctrs.containsKey(label))
			return t.isAssignableFrom(ctrs.get(label));
		if (addons != null && addons.containsKey(label))
			return t.isAssignableFrom(addons.get(label));
		Field f = RefUtils.getPrivateField(type.getJavaClass(), label);
		if (f != null) return t.isAssignableFrom(f.getType());
		return true;
	}

	private boolean needSetter(String label)
	{
		if (ctrs != null && ctrs.containsKey(label)) return false;
		if (addons != null && addons.containsKey(label)) return false;
		return getters.containsKey(label);
	}

	public Map<String, Class> getAddonsMap()
	{
		return addons;
	}

	public Map<String, Class> getCtrsMap()
	{
		return ctrs;
	}

	public Map<String, EventSetDescriptor> getEventSetDescriptorsMap()
	{
		return esd;
	}

	public Map<String, Method> getGettersMap()
	{
		return getters;
	}

	public Map<String, Field> getPrivFieldsMap()
	{
		return priv_fields;
	}

	public Map<String, Field> getPubFieldsMap()
	{
		return pub_fields;
	}

	public Map<String, Method> getSettersMap()
	{
		return setters;
	}

	boolean isEmptyMakeMethod(){
		return (setters.isEmpty() && pub_fields.isEmpty() 
				&& esd.isEmpty());
	} 
	
	boolean isEmptyStoreMethod(){
		return (getters.isEmpty() && pub_fields.isEmpty() 
				&& esd.isEmpty() && priv_fields.isEmpty());
	}

}
