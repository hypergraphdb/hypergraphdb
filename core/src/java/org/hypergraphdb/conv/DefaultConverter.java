package org.hypergraphdb.conv;

import java.beans.BeanInfo;
import java.beans.EventSetDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.JMenuItem;
import javax.swing.JTabbedPane;
import org.hypergraphdb.HGException;
import org.hypergraphdb.type.BonesOfBeans;

public class DefaultConverter implements Converter
{
	public static final String LISTENERS_KEY = "Listeners";
	protected String[] ctrParamNames = new String[0];
	protected Class[] ctrParamTypes;
	protected Map<String, Class> map = new HashMap<String, Class>();
	protected Constructor ctr;
	protected final Class type;
	protected Method factoryMethod;

	public DefaultConverter(Class type)
	{
		this(type, null);
	}

	public DefaultConverter(Class type, String[] ctrParamNames)
	{
		this.type = type;
		this.ctrParamNames = ctrParamNames;
		initMap();
	}

	public DefaultConverter(String cls, String[] ctrParamNames)
	{
		this(cls, ctrParamNames, null);
	}

	public DefaultConverter(String cls, String[] ctrParamNames,
			Class[] ctrParamTypes)
	{
		try
		{
			this.type = Class.forName(cls);
		}
		catch (Exception ex)
		{
			throw new RuntimeException("Can't create DefaultConverter " + ex);
		}
		this.ctrParamNames = ctrParamNames;
		this.ctrParamTypes = ctrParamTypes;
		initMap();
	}

	public Method getFactoryCtr()
	{
		return factoryMethod;
	}

	public void setFactoryCtr(Class cls, String method, String[] ctrParamNames,
			Class[] ctrParamTypes)
	{
		this.ctrParamNames = ctrParamNames;
		this.ctrParamTypes = ctrParamTypes;
		try
		{
			initCtrTypes(type);
			factoryMethod = type.getMethod(method, ctrParamTypes);
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			throw new RuntimeException(
					"Unable to construct converter for cls: " + type.getName());
		}
	}

	private void initMap()
	{
		if (ctrParamNames != null) initCtr();
	}

	private void initCtr()
	{
		try
		{
			// System.out.println("DefaultConverter-init: " + type.getName());
			initCtrTypes(type);
			ctr = type.getDeclaredConstructor(ctrParamTypes);
		}
		catch (Exception ex)
		{
			throw new RuntimeException(
					"Unable to construct converter for cls: " + type.getName());
		}
	}

	private void initCtrTypes(Class<?> type)
	{
		Map<String, Class<?>> super_map = MetaData.getConverter(
				type.getSuperclass()).getSlots();
		for (int i = 0; i < ctrParamNames.length; i++)
		{
			// System.out.println("DefC-pbpb: " + type + ":" + ctrParamNames[i]
			// +
			// ":" + ctrParamTypes[i]);
			PropertyDescriptor pd = BonesOfBeans.getPropertyDescriptor(type,
					ctrParamNames[i]);
			if (ctrParamTypes == null)
				ctrParamTypes = new Class[ctrParamNames.length];
			if (isAcceptable(type, pd))
			{
				if (ctrParamTypes[i] == null)
					ctrParamTypes[i] = pd.getPropertyType();
				continue;
			}
			if (super_map.get(ctrParamNames[i]) != null)
			{
				if (ctrParamTypes[i] == null)
					ctrParamTypes[i] = super_map.get(ctrParamNames[i]);
				continue;
			}
			if (ctrParamTypes[i] == null)
				ctrParamTypes[i] = RefUtils.getType(type, ctrParamNames[i]);
			map.put(ctrParamNames[i], ctrParamTypes[i]);
			// System.out.println("DefC-pbpb: " + type + ":" + ctrParamNames[i]
			// +
			// ":" + pd + ":" + ctrParamTypes[i]);
			if (ctrParamTypes[i] == null)
				throw new HGException("Unable to resolve field: "
						+ ctrParamNames[i] + " for class: " + type.getName());
			// System.out.println("DefaultConverter-pbpb: " + ctrParamNames[i] +
			// ":" + cls);
		}
	}
	Map<String, Class<?>> cachedSlots;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.hypergraphdb.conv.Converter#getSlots()
	 */
	public Map<String, Class<?>> getSlots()
	{
		// if (cachedSlots != null)
		// return cachedSlots;
		cachedSlots = new HashMap<String, Class<?>>();
		BeanInfo info = MetaData.getBeanInfo(type);
		// Properties
		PropertyDescriptor[] pd = info.getPropertyDescriptors();
		for (int i = 0; i < pd.length; ++i)
			if (isAcceptable(type, pd[i]))
				cachedSlots.put(pd[i].getName(), pd[i].getPropertyType());
		// System.out.println("Props: " + type + ":" + cachedSlots);
		// Public fields
		Field[] fs = type.getFields();
		for (Field f : fs)
			if (Modifier.isPublic(f.getModifiers())
					&& !Modifier.isStatic(f.getModifiers()))
				cachedSlots.put(f.getName(), f.getType());
		// EventListeners
		// if (java.awt.Component.class.isAssignableFrom(type)){
		EventSetDescriptor[] eventSetDescriptors = info
				.getEventSetDescriptors();
		for (int e = 0; e < eventSetDescriptors.length; e++)
		{
			EventSetDescriptor d = eventSetDescriptors[e];
			Class listenerType = d.getListenerType();
			cachedSlots.put(d.getName() + LISTENERS_KEY, listenerType);
		}
		// }
		for (Converter c : getClassHierarchy(type))
		{
			Map<String, Class<?>> inner = c.getSlots();
			if (inner != null) 
				for (String key : inner.keySet())
				  cachedSlots.put(key, inner.get(key));
		}
		Map<String, Class> inner = getAuxSlots();
		if (inner != null) for (String key : inner.keySet())
			cachedSlots.put(key, inner.get(key));
		Set<AddOnType> adds = getAllAddOnFields();
		if (adds != null)
			for (AddOnType a : adds)
				for (int i = 0; i < a.getArgs().length; i++)
					if (!cachedSlots.containsKey(a.getArgs()[i]))
					{
						if (!cachedSlots.containsKey(a.getArgs()[i]))
						{
							if (a.getTypes() != null)
								cachedSlots
										.put(a.getArgs()[i], a.getTypes()[i]);
							else
								cachedSlots.put(a.getArgs()[i], RefUtils
										.getType(type, a.getArgs()[i]));
						}
					}
		return cachedSlots;
	}
	private List<Converter> conv_list;

	private List<Converter> getClassHierarchy(Class t)
	{
		if (conv_list != null) return conv_list;
		conv_list = new LinkedList<Converter>();
		t = t.getSuperclass();
		while (t != null)
		{
			Converter c = MetaData.getConverter(t);
			if (c != null) conv_list.add(c);
			t = t.getSuperclass();
		}
		// Collections.reverse(conv_list);
		return conv_list;
	}
	
	private List<Converter> getReverseClassHierarchy(Class t)
	{
		List<Converter> list = getClassHierarchy(t);
		Collections.reverse(list);
		return list;
	}

	protected Map<String, Class> getAuxSlots()
	{
		return map;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.hypergraphdb.conv.Converter#make(java.lang.Class, java.util.Map)
	 */
	public Object make(Map<String, Object> props)
	{
		Object instance = instantiate(props);
		if (instance == null) return null;
		makeBean(instance, props);
		ex_make(instance, props);
		return instance;
	}

	protected void ex_make(Object instance, Map<String, Object> props)
	{
		if (type == null) return;
		for (Converter c : getClassHierarchy(type))
			if (c instanceof DefaultConverter)
				((DefaultConverter) c).ex_make(instance, props);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.hypergraphdb.conv.Converter#instantiate(java.util.Map)
	 */
	public Object instantiate(Map<String, Object> props)
	{
		// System.out.println(type.getName() + ":" + props);
		int nArgs = (ctrParamNames != null) ? ctrParamNames.length : 0;
		Object[] constructorArgs = new Object[nArgs];
		Class[] params = new Class[nArgs];
		for (int i = 0; i < nArgs; i++)
			constructorArgs[i] = props.get(ctrParamNames[i]);
		try
		{
			if (ctr == null) ctr = type.getDeclaredConstructor(params);
			ctr.setAccessible(true);
			return ctr.newInstance(constructorArgs);
		}
		catch (Exception e)
		{
			for (int i = 0; i < params.length; i++)
			{
				if (params[i] == null)
					System.err.println("NullParam for: " + ctrParamNames[i]
							+ ":" + type + ":" + props);
				params[i] = BonesOfBeans.primitiveEquivalentOf(params[i]);
			}
			try
			{
				ctr = type.getDeclaredConstructor(params);
				ctr.setAccessible(true);
				return ctr.newInstance(constructorArgs);
			}
			catch (Exception ex)
			{
				System.err.println("CTR: " + type + ":" + ex.toString());
				for (int i = 0; i < constructorArgs.length; i++)
				{
					System.err.println("args: " + constructorArgs[i]);
				}
				return null;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.hypergraphdb.conv.Converter#store(java.lang.Object)
	 */
	public Map<String, Object> store(Object instance)
	{
		Map<String, Object> props = new HashMap<String, Object>();
		storeBean(instance.getClass(), instance, props);
		ex_store(instance, props);
		return props;
	}

	protected void ex_store(Object instance, Map<String, Object> props)
	{
		// if (type == null) return;
		for (String key : map.keySet())
			putField(key, type, instance, props);
		for (Converter c : getClassHierarchy(type))
			if (c instanceof DefaultConverter)
				((DefaultConverter) c).ex_store(instance, props);
		Set<AddOnType> adds = getAddOnFields();
		if (adds != null) for (AddOnType a : adds)
			for (int i = 0; i < a.getArgs().length; i++)
				if (!props.containsKey(a.getArgs()[i]))
				{
					putField(a.getArgs()[i], type, instance, props);
				}
	}

	private static void putField(String name, Class type, Object instance,
			Map<String, Object> props)
	{
		props.put(name, RefUtils.getValue(instance, type, name));
		// System.out.println("putField: " + name + ":" + props.get(name));
	}

	// Write out the properties of this instance.
	protected void makeBean(Object oldInstance, Map<String, Object> props)
	{
		// System.out.println("initBean: " + oldInstance);
		BeanInfo info = MetaData.getBeanInfo(type);
		// Properties
		PropertyDescriptor[] propertyDescriptors = info
				.getPropertyDescriptors();
		for (int i = 0; i < propertyDescriptors.length; ++i)
		{
			try
			{
				makeProperty(propertyDescriptors[i], oldInstance, props);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		Field[] fs = type.getFields();
		for (Field f : fs)
			if (Modifier.isPublic(f.getModifiers())
					&& !Modifier.isStatic(f.getModifiers()))
				if (props.containsKey(f.getName())) try
				{
					f.set(oldInstance, props.get(f.getName()));
				}
				catch (IllegalAccessException ex)
				{
				}
		EventSetDescriptor[] eventSetDescriptors = info
				.getEventSetDescriptors();
		for (int e = 0; e < eventSetDescriptors.length; e++)
		{
			EventSetDescriptor d = eventSetDescriptors[e];
			Class listenerType = d.getListenerType();
			if (listenerType == java.awt.event.ComponentListener.class)
				continue;
			if (listenerType == javax.swing.event.ChangeListener.class
					&& type == javax.swing.JMenuItem.class) continue;
			EventListener[] l = (EventListener[]) props.get(d.getName()
					+ LISTENERS_KEY);
			if (l == null) continue;
			try
			{
				Method m = d.getAddListenerMethod();
				for (EventListener el : l)
					m.invoke(oldInstance, new Object[] { el });
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}
		}
	}

	// Write out the properties of this instance.
	private void storeBean(Class type, Object oldInstance,
			Map<String, Object> props)
	{
		// System.out.println("initBean: " + oldInstance);
		BeanInfo info = MetaData.getBeanInfo(type);
		// Properties
		PropertyDescriptor[] propertyDescriptors = info
				.getPropertyDescriptors();
		for (int i = 0; i < propertyDescriptors.length; ++i)
		{
			try
			{
				doProperty(type, propertyDescriptors[i], oldInstance, props);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		Field[] fs = type.getFields();
		for (Field f : fs)
			if (Modifier.isPublic(f.getModifiers())
					&& !Modifier.isStatic(f.getModifiers()))
				props.put(f.getName(), RefUtils.getValue(oldInstance,
						oldInstance.getClass(), f.getName()));
		// Listeners
		/*
		 * Pending(milne). There is a general problem with the archival of
		 * listeners which is unresolved as of 1.4. Many of the methods which
		 * install one object inside another (typically "add" methods or
		 * setters) automatically install a listener on the "child" object so
		 * that its "parent" may respond to changes that are made to it. For
		 * example the JTable:setModel() method automatically adds a
		 * TableModelListener (the JTable itself in this case) to the supplied
		 * table model.
		 * 
		 * We do not need to explictly add these listeners to the model in an
		 * archive as they will be added automatically by, in the above case,
		 * the JTable's "setModel" method. In some cases, we must specifically
		 * avoid trying to do this since the listener may be an inner class that
		 * cannot be instantiated using public API.
		 * 
		 * No general mechanism currently exists for differentiating between
		 * these kind of listeners and those which were added explicitly by the
		 * user. A mechanism must be created to provide a general means to
		 * differentiate these special cases so as to provide reliable
		 * persistence of listeners for the general case.
		 */
		if (!java.awt.Component.class.isAssignableFrom(type))
		{
			return; // Just handle the listeners of Components for now.
		}
		EventSetDescriptor[] eventSetDescriptors = info
				.getEventSetDescriptors();
		for (int e = 0; e < eventSetDescriptors.length; e++)
		{
			EventSetDescriptor d = eventSetDescriptors[e];
			Class listenerType = d.getListenerType();
			// The ComponentListener is added automatically, when
			// Contatiner:add is called on the parent.
			if (listenerType == java.awt.event.ComponentListener.class)
			{
				continue;
			}
			// JMenuItems have a change listener added to them in
			// their "add" methods to enable accessibility support -
			// see the add method in JMenuItem for details. We cannot
			// instantiate this instance as it is a private inner class
			// and do not need to do this anyway since it will be created
			// and installed by the "add" method. Special case this for now,
			// ignoring all change listeners on JMenuItems.
			if (listenerType == javax.swing.event.ChangeListener.class
					&& type == javax.swing.JMenuItem.class)
			{
				continue;
			}
			EventListener[] oldL = new EventListener[0];
			try
			{
				Method m = d.getGetListenerMethod();
				oldL = (EventListener[]) m.invoke(oldInstance, new Object[] {});
			}
			catch (Throwable e2)
			{
				try
				{
					Method m = type.getMethod("getListeners",
							new Class[] { Class.class });
					oldL = (EventListener[]) m.invoke(oldInstance,
							new Object[] { listenerType });
					// System.out.println("storeBean -listener: " + oldL);
				}
				catch (Exception e3)
				{
					return;
				}
			}
			// System.out.println("addListeners: " + d.getName() + ":" + oldL);
			props.put(d.getName() + LISTENERS_KEY, filterListeners(oldInstance,
					oldL));
		}
	}

	protected EventListener[] filterListeners(Object instance,
			EventListener[] in)
	{
		Set<EventListener> res = new HashSet<EventListener>(in.length);
		for (EventListener e : in)
		{
			if (e.getClass().getName().startsWith("javax.swing.plaf."))
			{
				System.err.println("Filtering " + e);
				continue;
			}
			if (e.getClass().isMemberClass()
					&& !Modifier.isStatic(e.getClass().getModifiers()))
			{
				System.err.println("Filtering " + e);
				continue;
			}
			// normally those listeners will be added during construction
			if (e.getClass().equals(instance.getClass()))
			{
				System.err.println("Filtering " + e);
				continue;
			}
			if (e.getClass().getEnclosingClass() == JMenuItem.class
					&& e.getClass().getName().startsWith(
							"javax.swing.JMenuItem$"))
			{
				System.err.println("Filtering " + e);
				continue;
			}
			res.add(e);
		}
		return res.toArray(new EventListener[res.size()]);
	}

	private void doProperty(Class type, PropertyDescriptor pd, Object instance,
			Map<String, Object> props) throws Exception
	{
		Method getter = pd.getReadMethod();
		if (isAcceptable(type, pd))
			props.put(pd.getName(), invokeMethod(instance, getter,
					new Object[] {}));
	}

	private void makeProperty(PropertyDescriptor pd, Object oldInstance,
			Map<String, Object> props) throws Exception
	{
		Method setter = pd.getWriteMethod();
		if (isAcceptable(type, pd))
		{
			Object val = props.get(pd.getName());
			try
			{
				if (val != null)
					setter.invoke(oldInstance, new Object[] { val });
				// System.err.println("Property: " + pd.getName() + ": " + val);
			}
			catch (Exception ex)
			{
				System.err.println("Unable to set property: " + pd.getName()
						+ ": " + val + ": "
						+ ((val != null) ? val.getClass() : "") + ":" + type
						+ ". Reason: " + ex);
				// ex.printStackTrace();
			}
		}
	}

	static Object invokeMethod(Object instance, Method method, Object[] args)
	{
		try
		{
			return method.invoke(instance, args);
		}
		catch (Exception ex)
		{
			return null;
		}
	}

	// This is a workaround for a bug in the introspector.
	// PropertyDescriptors are not shared amongst subclasses.
	static boolean isTransient(Class type, PropertyDescriptor pd)
	{
		if (type == null) return false;
		// System.out.println("isTransient: " + type.getName()+ ":" +
		// pd.getName() + ":" + pd.getValue("transient"));
		// This code was mistakenly deleted - it may be fine and
		// is more efficient than the code below. This should
		// all disappear anyway when property descriptors are shared
		// by the introspector.
		Method getter = pd.getReadMethod();
		Class declaringClass = getter.getDeclaringClass();
		if (declaringClass == type)
		{
			return Boolean.TRUE.equals(pd.getValue("transient"));
		}
		String pName = pd.getName();
		BeanInfo info = MetaData.getBeanInfo(type);
		PropertyDescriptor[] propertyDescriptors = info
				.getPropertyDescriptors();
		for (int i = 0; i < propertyDescriptors.length; ++i)
		{
			PropertyDescriptor pd2 = propertyDescriptors[i];
			if (pName.equals(pd2.getName()))
			{
				Object value = pd2.getValue("transient");
				if (value != null)
				{
					return Boolean.TRUE.equals(value);
				}
			}
		}
		return isTransient(type.getSuperclass(), pd);
	}

	static boolean isAcceptable(Class type, PropertyDescriptor pd)
	{
		return pd != null && pd.getReadMethod() != null
				&& pd.getWriteMethod() != null && !isTransient(type, pd);
	}

	public Constructor getCtr()
	{
		return ctr;
	}

	public String[] getCtrArgs()
	{
		return ctrParamNames;
	}

	public void setCtr(Constructor ctr)
	{
		this.ctr = ctr;
	}

	public Set<AddOnType> getAddOnFields()
	{
		return null;
	}
	
    private Set<AddOnType> addons;
	public Set<AddOnType> getAllAddOnFields(){
		if(addons != null) return addons;
		addons = new HashSet<AddOnType>();
		List<Converter> convs = getReverseClassHierarchy(type);
		for(Converter c: convs){
			Set<AddOnType> s = c.getAddOnFields();
			if(s == null) continue;
			for(AddOnType a: s)
				addons.add(a);
		}
		Set<AddOnType> s = getAddOnFields();
		if(s != null)
		   for(AddOnType a: s)
			   addons.add(a);
		return addons;
	}

	public Class[] getCtrTypes()
	{
		return ctrParamTypes;
	}

	public Class getType()
	{
		return type;
	}
}
