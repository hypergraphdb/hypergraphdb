package org.hypergraphdb.conv.types;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.GridBagLayout;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.JTabbedPane;
import javax.swing.tree.MutableTreeNode;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.conv.RefUtils;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.type.Record;
import org.hypergraphdb.type.Slot;
import org.objectweb.asm.MethodVisitor;

public class AddOnFactory {

	static String[] gbNames = { java.awt.BorderLayout.NORTH,
		java.awt.BorderLayout.SOUTH, java.awt.BorderLayout.EAST,
		java.awt.BorderLayout.WEST, java.awt.BorderLayout.CENTER };

	public static final String ADD_STR = "add_string";
    public static final String ADD_COMP = "add_comp";
    public static final String ADD_TREE = "add_tree";
	public static final String ADD_TAB = "add_tab";
	public static final String ADD_EL = "addElement";

	public static final String CARD_LAYOUT = "addLayout";
	public static final String GRID_BAG_LAYOUT = "gridBagLayout";
	public static final String BORDER_LAYOUT = "borderLayout";
	
	
	static Map<String, Class[]> linkTypes = new HashMap<String, Class[]>();
	static{
		linkTypes.put(ADD_STR, new Class[] { String.class });
		linkTypes.put(ADD_COMP, new Class[] { Component.class });
		linkTypes.put(ADD_TREE, new Class[] { MutableTreeNode.class });
		
		linkTypes.put(ADD_EL, new Class[] {Vector.class});

		linkTypes.put(CARD_LAYOUT, new Class[] {Vector.class});
		linkTypes.put(GRID_BAG_LAYOUT, new Class[] {Hashtable.class});
		linkTypes.put(BORDER_LAYOUT, new Class[] {Component.class, 
				Component.class, Component.class, Component.class, Component.class});
	}
	
    public static Class[] getLinkTypes(HGRelType link){
    	return linkTypes.get(link.getName());
	}
    
	static void processLink(HyperGraph hg, HGRelType link, 
			Record record, Object instance) {
		String name = link.getName();
		if (ADD_STR.equals(name) || ADD_COMP.equals(name)
				||ADD_TREE.equals(name)) {
			addChildren(hg, (HGRelType) link, record, instance);
		} else if (ADD_EL.equals(name)) {
			Vector value = (Vector) getLinkValue(hg, link, record);
			if (value == null)
				return;
			boolean combo = instance instanceof	DefaultComboBoxModel; 
			for (Object o : value)
				if(combo)
					((DefaultComboBoxModel)instance).addElement(o);
                else
					 ((DefaultListModel)instance).addElement(o);
		} else if (CARD_LAYOUT.equals(name)) {
			CardLayout c = ((CardLayout) instance);
			Vector items = (Vector) getLinkValue(hg, link, record);
			if (items != null)
				for (int i = 0; i < items.size(); i++)
					if (items.get(i) != null) {
						Object card = items.get(i);
						String n = (String) RefUtils.getValue(card, card
								.getClass(), "name");
						Component comp = (Component) RefUtils.getValue(
								card, card.getClass(), "comp");
						c.addLayoutComponent(comp, n);
					}
		}else if (GRID_BAG_LAYOUT.equals(name)) {
			Hashtable comptable = (Hashtable)getLinkValue(hg, link, record);
			if (comptable != null) {
				GridBagLayout c = ((GridBagLayout) instance);
				for (Enumeration e = comptable.keys(); e.hasMoreElements();) {
					Component child = (Component) e.nextElement();
					c.addLayoutComponent(child, comptable.get(child));
				}
			}
		}else if(BORDER_LAYOUT.equals(name)){
			Object[] items = getLinkValues(hg, link, record);
			BorderLayout c = ((BorderLayout) instance);
			if (c!= null && items != null)
				for (int i = 0; i < gbNames.length; i++)
					if (items[i] != null)
						c.addLayoutComponent((Component)items[i], gbNames[i]);
		}
		else if (ADD_TAB.equals(name)) {
			JTabbedPane c = ((JTabbedPane) instance);
			Object[] values = getLinkValues(hg, link, record);
			Component[] items = (Component[]) values[0];
			String[] titles = (String[]) values[1];
			Icon[] icons = (Icon[]) values[2];
			if (items != null && titles != null && icons != null)
				for (int i = 0; i < items.length; i++)
					c.addTab(titles[i], icons[i], items[i]);
		}
	}

	static void addChildren(HyperGraph hg, HGRelType link, 
			Record record, Object instance) {
		try {
			if (instance == null)	return;
			Object value = getLinkValue(hg, link, record);
			System.out.println("AddonF - addChildren1:" + instance.getClass() + ":" +
					link.getName() + ":" + value);
			
			if (value == null)	return;
			Class[] args = getLinkTypes(link); // (Class[]) hg.get(link.getTargetAt(1));
			
			Method m = instance.getClass().getMethod("add", args);
			if (value.getClass().isArray()) {
				Object[] array = (Object[]) value;
				for (int i = 0; i < array.length; i++) {
					if(array[i] != null)
					   m.invoke(instance, array[i]);
				}
			} else if (value.getClass().isAssignableFrom(Collection.class)) {

				Collection c = (Collection) instance;
				for (Object o : c)
					m.invoke(instance, o);
			} else{
				System.out.println("AddonF - addChildren:" + instance +
						":" + value);
				m.invoke(instance, value);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	static Object instantiateFactoryConstructorLink(HyperGraph hg, SwingType type, FactoryConstructorLink link, Record record)
	{
		Class[] types = new Class[0];
		Object[] args = new Object[0];
		int nArgs = link.getArity() - 2;
		args = new Object[nArgs];
		types = new Class[nArgs];
		Class c = (Class) hg.get(link.getTargetAt(0));
		String method_name = (String) hg.get(link.getTargetAt(1));
		for (int i = 0; i < nArgs; i++)
		{
			HGValueLink l = (HGValueLink) hg.get(link.getTargetAt(i + 2));
			types[i] = (Class) l.getValue();
			Slot s = (Slot) hg.get(l.getTargetAt(0));
			args[i] = record.get(s);
		   // System.out.println("AddOnFactory - instantiate - args: " + types[i]);
		}
		try{
			return c.getMethod(method_name, types).invoke(null, args);
		}catch(Exception ex){
			throw new HGException("Unable to instantiate method: " + method_name +
					" on " + c.getName() + ". Reason: " + ex);
		}
	}
	
	static Object instantiateConstructorLink(HyperGraph hg, SwingType type, 
			ConstructorLink link, Record record)
	{
		Class[] types = new Class[0];
		Object[] args = new Object[0];
		if (link != null) {
			int nArgs = link.getArity();
			args = new Object[nArgs];
			types = new Class[nArgs];
			for (int i = 0; i < nArgs; i++) {
				HGValueLink l = (HGValueLink) hg.get(link.getTargetAt(i));
				types[i] = (Class) l.getValue();
				Slot s = (Slot) hg.get(l.getTargetAt(0));
				args[i] = record.get(s);
				//System.out.println("SB - instantiate - args: " + s.getLabel() + ":" +
				//		args[i]);
			}
		}
		
		Class beanClass = type.getJavaClass();
		Constructor ctr = null;
		try {
			ctr = beanClass.getDeclaredConstructor(types);
			ctr.setAccessible(true);
			return ctr.newInstance(args);
		} catch (Exception e) {
			for (int i = 0; i < types.length; i++) {
				if (types[i] == null)
					System.err.println("NullParam at index: " + i + ":"
							+ beanClass);
				types[i] = BonesOfBeans.primitiveEquivalentOf(types[i]);
			}
			try {
				ctr = beanClass.getDeclaredConstructor(types);
				ctr.setAccessible(true);
				return ctr.newInstance(args);
			} catch (Exception ex) {
				System.err.println("CTR: " + beanClass + ":" + ex.toString());
				for (int i = 0; i < args.length; i++) {
					System.err.println("args: " + args[i] + ":" + types[i]);
				}
				return null;
			}
		}
	}
	

	static Object getLinkValue(HyperGraph hg, HGLink link, Record record)
	{
		Slot s = (Slot) hg.get(link.getTargetAt(0));
		//System.out.println("AddonF - getLinkValue:" + s.getLabel() +
		//		hg.get(s.getValueType()) + ":" + record.get(s));
		return record.get(s);
	}
	
	static Object[] getLinkValues(HyperGraph hg, HGLink link, Record record)
	{
		Object[] values = new Object[link.getArity()];
		for (int i = 0; i < values.length; i++) {
			Slot slot = (Slot) hg.get(link.getTargetAt(i));
			values[i] = record.get(slot);
		}
		return values;
	}

	public static Map<String, Class> getAddOnSlots(HyperGraph hg, SwingType type)
	{
		//System.out.println("getAddOnSlots00: " + type.getJavaClass() + ":");
		
		AddOnLink addons = (AddOnLink) hg.get(type.getAddOnsHandle());
		//System.out.println("getAddOnSlots01: " + type + ":" + addons);
		
		if (addons == null) return null;
		Map<String, Class> res = new HashMap<String, Class>();
		for (int i = 0; i < addons.getArity(); i++)
		{
			HGRelType link = (HGRelType) hg.get(addons.getTargetAt(i));
			Class[] types = getLinkTypes(link);
			if(types == null) continue;
			for(int j=0; j<link.getArity(); j++){
				Slot s = (Slot) hg.get(link.getTargetAt(j));
				if(s != null)
				  res.put(s.getLabel(), types[j]);
				else
					System.err.println("NULL AddOnSlot at: " + i + ":" + type.getJavaClass());
			}
		}
		return res;
	}
	
	interface AddOnLinkDescriptor{
		String getName();
		Class[] getTypes();
		void processLink(HyperGraph hg, HGRelType link, 
				Record record, Object instance);
		void save(HyperGraph hg, HGRelType link, 
				Record record, Object instance);
		boolean supportsASM();
		void asmCode(MethodVisitor mv);
	}
	
}