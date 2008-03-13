package org.hypergraphdb.conv;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.swing.DefaultCellEditor;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JTextField;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.conv.types.AddOnFactory;
import org.hypergraphdb.conv.types.AddOnLink;
import org.hypergraphdb.conv.types.ConstructorLink;
import org.hypergraphdb.conv.types.FactoryConstructorLink;
import org.hypergraphdb.conv.types.SwingType;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.type.Record;
import org.hypergraphdb.type.Slot;

public class RefUtils
{
	public static Method getGetMethod(final Class clazz, final String name)
	{
		final String fieldName = name.substring(0, 1).toUpperCase()
				+ name.substring(1);
		Method getMethod = null;
		try
		{
			getMethod = clazz.getMethod("get" + fieldName, new Class[] {});
		}
		catch (final Exception e)
		{
		}
		if (getMethod == null) try
		{
			getMethod = clazz.getMethod("is" + fieldName, new Class[] {});
		}
		catch (final Exception e)
		{
		}
		return getMethod;
	}

	public static Method getSetMethod(final Class clazz, final String name)
	{
		final String fieldName = setterName(name);
		Method setMethod = null;
		try
		{
			Method m = getGetMethod(clazz, name);
			if (m != null)
				setMethod = clazz.getMethod(fieldName, new Class[] { m
						.getReturnType() });
		}
		catch (final Exception e)
		{
		}
		return setMethod;
	}

	public static String setterName(String name)
	{
		return "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
	}

	public static Field getPublicField(Class cls, String name)
	{
		try
		{
			Field f = cls.getField(name);
			if (f != null && !Modifier.isStatic(f.getModifiers()) &&
					Modifier.isPublic(f.getModifiers()))
				return f;
		}
		catch (Exception e)
		{
		}
		return null;
	}

	public static Field getPrivateField(Class cls, String name)
	{
		try
		{
			Field f = cls.getDeclaredField(name);
			if (f != null &&
					!Modifier.isStatic(f.getModifiers())) 
				return f;
		}
		catch (Exception e)
		{
		}
		return (cls.getSuperclass() == null) ? null : getPrivateField(cls
				.getSuperclass(), name);
	}

	public static Object getPrivateFieldValue(Object instance, Class cls,
			String name)
	{
		int dot = name.indexOf(".");
		if (dot > 0) return getValue(instance, cls, name);
		try
		{
			Field f = getPrivateField(cls, name);
			if (f != null)
			{
				f.setAccessible(true);
				return f.get(instance);
			}
		}
		catch (Exception e)
		{
			System.err.println("Unable to get field: " +
					name + " in class: " + cls.getName());
		}
		return null;
	}
	
	public static void setPrivateFieldValue(Object instance, Class cls,
			String name, Object value)
	{
		int dot = name.indexOf(".");
		if (dot > 0) {
			return; //getValue(instance, cls, name);
		}
		try
		{
			Field f = getPrivateField(cls, name);
			if (f != null)
			{
				f.setAccessible(true);
				f.set(instance, value);
			}
		}
		catch (Exception e)
		{
			System.err.println("Unable to set: " + value + "for " +
					name + " in class: " + cls.getName());
		}
	}

	public static Class getType(Class cls, String name)
	{
		int dot = name.indexOf(".");
		if (dot > 0)
		{
			Class c = getType(cls, name.substring(0, dot));
			return getType(c, name.substring(dot + 1));
		}
		Field f = getPublicField(cls, name);
		if (f != null) return f.getType();
		Method m = getGetMethod(cls, name);
		if (m != null) return m.getReturnType();
		f = getPrivateField(cls, name);
		return (f != null) ? f.getType() : null;
	}

	public static Object getValue(Object instance, Class cls, String name)
	{
		int dot = name.indexOf(".");
		if (dot > 0)
		{
			Object o = getValue(instance, cls, name.substring(0, dot));
			return getValue(o, o.getClass(), name.substring(dot + 1));
		}
		try
		{
			Field f = getPublicField(cls, name);
			if (f != null)
				return f.get(instance);
			f = getPrivateField(cls, name);
			if (f != null)
			{
				f.setAccessible(true);
				return f.get(instance);
			}
			Method m = getGetMethod(cls, name);
			if (m != null) return m.invoke(instance);
		}
		catch (Exception e)
		{
			System.err.println("Unable to retrieve field: " + name + " for "
					+ instance + "in " + cls + " Reason: " + e);
			//e.printStackTrace();
		}
		return null;
	}

	private RefUtils()
	{
	}

	public static BeanInfo getBeanInfo(Class type)
	{
		BeanInfo info = null;
		try
		{
			info = Introspector.getBeanInfo(type);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
		}
		return info;
	}

	
}
