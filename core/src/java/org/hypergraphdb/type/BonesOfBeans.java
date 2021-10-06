/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import java.beans.Introspector;
import java.beans.BeanInfo;
import java.beans.PropertyDescriptor;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hypergraphdb.HGException;

/**
 * This is a utility class to handle bean introspection. All functions may throw
 * a <code>HGException</code> which will wrap the underlying bean introspection
 * and reflection (for set and get) exceptions.
 * 
 */
public class BonesOfBeans
{
	// Cache bean property descriptors as soon as they are fetched. The cache is
	// only
	// being added, never freed. The assumption is number of bean classes used
	// is finite and not very big. It is not very hard to keep only a constant
	// max number of cached property descriptors, but then we will have to
	// synchronize the cache and THAT will slow things down...
	// static private HashMap<Class, HashMap<String, PropertyDescriptor>> cache
	// =
	// new HashMap<Class, HashMap<String, PropertyDescriptor>>();

	private static Map<String, PropertyDescriptor> getpropmap(Class<?> clazz)
	{
		return getpropmap(clazz, false);
	}

	private static Map<String, PropertyDescriptor> getpropmap(Class<?> clazz,
			boolean incl_cls)
	{
		HashMap<String, PropertyDescriptor> propmap = null; // cache.get(clazz);
		if (propmap == null)
		{
			try
			{
				BeanInfo bean_info = Introspector.getBeanInfo(clazz);
				propmap = new HashMap<String, PropertyDescriptor>();
				PropertyDescriptor beanprops[] = bean_info
						.getPropertyDescriptors();
				for (int i = 0; i < beanprops.length; i++)
				{
					// filter the Class property which is not used
					if (!incl_cls && "class".equals(beanprops[i].getName()))
						continue;
					propmap.put(beanprops[i].getName(), beanprops[i]);
				}
				// cache.put(clazz, propmap);
			} catch (IntrospectionException ex)
			{
				throw new HGException("The bean " + clazz.getName()
						+ " doesn't want us to introspect it: " + ex.toString());
			}
		}
		return propmap;
	}

	private static PropertyDescriptor getorthrow_propdesc(Object bean,
			String propname, int index)
	{
		PropertyDescriptor desc = getPropertyDescriptor(bean, propname);
		if (desc == null)
			throw new HGException("Property " + propname
					+ " could not be found in bean "
					+ bean.getClass().getName());
		else if (index >= 0 && !(desc instanceof IndexedPropertyDescriptor))
			throw new HGException("Property " + propname
					+ " is not an indexed property in bean "
					+ bean.getClass().getName());
		else
			return desc;
	}

	/**
	 * @param beanClass
	 *            The bean class whose property descriptors are desired. Cannot
	 *            be <code>null</code>.
	 * 
	 * @return a <code>Map</code> of all property descriptors in the bean. The
	 *         map elements are keyed by property name.
	 */
	public static Map<String, PropertyDescriptor> getAllPropertyDescriptors(
			Class<?> beanClass)
	{
		return getpropmap(beanClass);
	}

	public static Map<String, PropertyDescriptor> getAllPropertyDescriptorsEx(
			Class<?> beanClass)
	{
		return getpropmap(beanClass, true);
	}

	/**
	 * @param bean
	 *            The bean whose property descriptors are desired. Cannot be
	 *            <code>null</code>
	 * 
	 * @return a <code>Map</code> of all property descriptors in the bean. The
	 *         map elements are keyed by property name.
	 */
	public static Map<String, PropertyDescriptor> getAllPropertyDescriptors(
			Object bean)
	{
		return getpropmap(bean.getClass());
	}

	/**
	 * @param bean
	 *            Any object conforming to the Java Beans Specification
	 * @param propname
	 *            The name a bean property.
	 * @return The <code>PropertyDescriptor</code> for that property as returned
	 *         from the <code>BeanInfo</code> associated with that bean. If
	 *         there is no property with that name in the bean,
	 *         <code>null</code> is returned.
	 */
	public static PropertyDescriptor getPropertyDescriptor(Object bean,
			String propname)
	{
		return getPropertyDescriptor(bean.getClass(), propname);
	}

	public static PropertyDescriptor getPropertyDescriptor(Class<?> clazz, String propname)
	{
		return (PropertyDescriptor) getpropmap(clazz).get(propname);		
	}
	
	/**
	 * Same as <code>getProperty(Object, String, int)</code>, but use a supplied
	 * <code>PropertyDescriptor</code> instead of the property name.
	 */
	public static Object getProperty(Object bean, PropertyDescriptor prop,
			int index)
	{
		try
		{
			Method method = null;
			if (prop instanceof IndexedPropertyDescriptor)
				if (index < 0)
					method = prop.getReadMethod();
				else
					method = ((IndexedPropertyDescriptor) prop)
							.getIndexedReadMethod();
			else if (index >= 0)
				throw new java.lang.UnsupportedOperationException("Property "
						+ prop.getName() + " of bean "
						+ bean.getClass().getName()
						+ " is not an indexed property.");
			else
				method = prop.getReadMethod();
			if (method == null)
				throw new java.lang.UnsupportedOperationException("Property "
						+ prop.getName() + " of bean "
						+ bean.getClass().getName() + " is not readable.");
			if (index < 0)
				return method.invoke(bean, (Object[]) null);
			else
				return method.invoke(bean, new Object[] { new Integer(index) });
		} catch (IllegalAccessException ex)
		{
			throw new HGException("Illegal access to property "
					+ prop.getName() + " of bean " + bean.getClass().getName()
					+ " it is probably a private property: " + ex.toString());
		} catch (InvocationTargetException ex)
		{
			throw new HGException("InvocationTargetException while accessing "
					+ "property " + prop.getName() + " of bean "
					+ bean.getClass().getName() + ex.toString()
					+ ", target exception is "
					+ ex.getTargetException().toString(), ex);
		}
	}

	/**
	 * <p>
	 * Return a Java Map of all read-write properties of the specified Java bean.
	 * </p>
	 */
	public static Map<String, Object> getPropertiesAsMap(Object bean)
	{
		HashMap<String, Object> m = new HashMap<String, Object>();
		Map<String, PropertyDescriptor> dm = getAllPropertyDescriptors(bean);
		for (Map.Entry<String, PropertyDescriptor> e : dm.entrySet())
			if (e.getValue().getWriteMethod() != null && e.getValue().getReadMethod() != null)
				m.put(e.getKey(), getProperty(bean, e.getValue()));
		return m;
	}

	public static void setPropertiesFromMap(Object bean, Map<String, Object> properties)
	{
		HashMap<String, Object> m = new HashMap<String, Object>();
		Map<String, PropertyDescriptor> dm = getAllPropertyDescriptors(bean);
		for (Map.Entry<String, PropertyDescriptor> e : dm.entrySet())
			if (e.getValue().getWriteMethod() != null && e.getValue().getReadMethod() != null)
				setProperty(bean, e.getKey(), properties.get(e.getKey()));
	}

	/**
	 * Same as <code>getProperty(Object, String)</code>, but use a supplied
	 * <code>PropertyDescriptor</code> instead of the property name.
	 */
	public static Object getProperty(Object bean, PropertyDescriptor prop)
	{
		return getProperty(bean, prop, -1);
	}

	/**
	 * Get the value of a particular (indexed) bean property.
	 * 
	 * @param bean
	 *            An object conforming to the Java Beans Specification.
	 * @param propname
	 *            The name of the desired bean property.
	 * @param index
	 *            The <code>integer</code> index within the property. If the
	 *            value is negative, the property itself is returned. That is,
	 *            for example, if the property is <code>String [] values</code>
	 *            and if <code>index</code> is < 0, <code>values</code> will be
	 *            returned, otherwise <code>values[index]</code> will be
	 *            returned.
	 * @return The value of the property at the specified index.
	 * 
	 * @throws HGException
	 *             if the property cannot be found in the bean or if
	 *             <code>index >= 0 </code> and the property is not an indexed
	 *             property.
	 */
	public static Object getProperty(Object bean, String propname, int index)
	{
		return getProperty(bean, getorthrow_propdesc(bean, propname, index),
				index);
	}

	/**
	 * Get the value of a particular bean property.
	 * 
	 * @param bean
	 *            An object conforming to the Java Beans Specification.
	 * @param propname
	 *            The name of the desired bean property.
	 * @return The value of the specified property.
	 * 
	 * @throws HGException
	 *             if the property cannot be found in the bean or if
	 *             <code>index >= 0 </code> and the property is not an indexed
	 *             property.
	 */
	public static Object getProperty(Object bean, String propname)
	{
		return getProperty(bean, propname, -1);
	}

	/**
	 * Same as <code>setProperty(Object, String, int, Object)</code>, but use a
	 * supplied <code>PropertyDescriptor</code> instead of the property name.
	 */
	public static void setProperty(Object bean, PropertyDescriptor prop,
			int index, Object newvalue)
	{
		try
		{
			Method method = null;
			if (prop instanceof IndexedPropertyDescriptor)
				if (index < 0)
					method = prop.getWriteMethod();
				else
					method = ((IndexedPropertyDescriptor) prop)
							.getIndexedWriteMethod();
			else if (index >= 0)
				throw new java.lang.UnsupportedOperationException("Property "
						+ prop.getName() + " of bean "
						+ bean.getClass().getName()
						+ " is not an indexed property.");
			else
				method = prop.getWriteMethod();
			if (method == null)
				throw new java.lang.UnsupportedOperationException("Property "
						+ prop.getName() + " of bean "
						+ bean.getClass().getName()
						+ " is not mutable (writeable).");
			if (index < 0)
				method.invoke(bean, new Object[] { newvalue });
			else
				method.invoke(bean,
						new Object[] { new Integer(index), newvalue });
		} catch (IllegalAccessException ex)
		{
			throw new HGException("Illegal access to property "
					+ prop.getName() + " of bean " + bean.getClass().getName()
					+ " it is probably a private property: " + ex.toString());
		} catch (InvocationTargetException ex)
		{
			throw new HGException("InvocationTargetException while accessing "
					+ "property " + prop.getName() + " of bean "
					+ bean.getClass().getName() + ex.toString()
					+ ", taget exception is "
					+ ex.getTargetException().toString(), ex);
		}
	}

	/**
	 * Same as <code>setProperty(Object, String, Object)</code>, but use a
	 * supplied <code>PropertyDescriptor</code> instead of the property name.
	 */
	public static void setProperty(Object bean, PropertyDescriptor prop,
			Object newvalue)
	{
		setProperty(bean, prop, -1, newvalue);
	}

	/**
	 * Assign a new value to a specified bean property.
	 * 
	 * @param bean
	 *            An object conforming to the Java Beans Specification.
	 * @param propname
	 *            The name of the desired bean property.
	 * @param index
	 *            The <code>integer</code> index within the property. If the
	 *            value is negative, the property itself will be modified. That
	 *            is, for example, if the property is
	 *            <code>String [] values</code> and if <code>index</code> is <
	 *            0, <code>values</code> will be modified, otherwise
	 *            <code>values[index]</code> will be modified.
	 * @param newvalue
	 *            The value to assign to the specified property.
	 * 
	 * @throws HGException
	 *             if the property cannot be found in the bean or if
	 *             <code>index >= 0 </code> and the property is not an indexed
	 *             property.
	 */
	public static void setProperty(Object bean, String propname, int index,
			Object newvalue)
	{
		setProperty(bean, getorthrow_propdesc(bean, propname, index), index,
				newvalue);
	}

	/**
	 * Assign a new value to a specified bean property.
	 * 
	 * @param bean
	 *            An object conforming to the Java Beans Specification.
	 * @param propname
	 *            The name of the desired bean property.
	 * @param newvalue
	 *            The value to assign to the specified property.
	 * 
	 * @throws HGException
	 *             if the property cannot be found in the bean or if
	 *             <code>index >= 0 </code> and the property is not an indexed
	 *             property.
	 */
	public static void setProperty(Object bean, String propname, Object newvalue)
	{
		setProperty(bean, propname, -1, newvalue);
	}

	/**
	 * Construct a new instance of a bean.
	 * 
	 * @param classname
	 *            The fully qualified class name of the bean to construct.
	 * @return The newly constructed bean.
	 * @throws HGException
	 *             is there's any error during the construction process, such as
	 *             ClassNotFoundException etc.
	 */
//	public static Object makeBean(String classname)
//	{
//		try
//		{
//			Class<?> clazz = Class.forName(classname);
//			return clazz.newInstance();
//		} catch (Exception ex)
//		{
//			throw new HGException("Could not construct a bean DataObject "
//					+ "from " + classname, ex);
//
//		}
//	}

	/**
	 * @param aClass
	 *            a Class
	 * @return the class's primitive equivalent, if aClass is a primitive
	 *         wrapper. If aClass is primitive, returns aClass. Otherwise,
	 *         returns null.
	 */
	public static Class<?> primitiveEquivalentOf(Class<?> aClass)
	{
		return aClass.isPrimitive() ? aClass : (Class<?>) objectToPrimitiveMap
				.get(aClass);
	}

	public static Class<?> wrapperEquivalentOf(Class<?> aClass)
	{
		Iterator<Map.Entry<Class<?>, Class<?>>> it = objectToPrimitiveMap
				.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<Class<?>, Class<?>> entry = it.next();
			if (aClass.equals(entry.getValue()))
				return (Class<?>) entry.getKey();
		}
		return aClass;
	}

	/**
	 * Mapping from primitive wrapper Classes to their corresponding primitive
	 * Classes.
	 */
	private static final Map<Class<?>, Class<?>> objectToPrimitiveMap = new HashMap<Class<?>, Class<?>>(
			13);

	static
	{
		objectToPrimitiveMap.put(Boolean.class, Boolean.TYPE);
		objectToPrimitiveMap.put(Byte.class, Byte.TYPE);
		objectToPrimitiveMap.put(Character.class, Character.TYPE);
		objectToPrimitiveMap.put(Double.class, Double.TYPE);
		objectToPrimitiveMap.put(Float.class, Float.TYPE);
		objectToPrimitiveMap.put(Integer.class, Integer.TYPE);
		objectToPrimitiveMap.put(Long.class, Long.TYPE);
		objectToPrimitiveMap.put(Short.class, Short.TYPE);
	}
}
