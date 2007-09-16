/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.beans.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.type.javaprimitive.EnumType;


/**
 * 
 * <p>
 * The <code>JavaTypeFactory</code> is used to infer HyperGraph types based on
 * Java beans. Java bean classes are converted to instances of
 * <code>RecordType</code> and Java bean instances to the corresponding
 * <code>Record</code>s. Only properties that are both readable and writeable
 * are mapped to HyperGraph <code>Slot</code>s.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class JavaTypeFactory {
	private static JavaTypeFactory instance = new JavaTypeFactory();

	public static interface PropertyVisitor {
		void onProperty(String name, Class type, Object value);
	}

	public static JavaTypeFactory getInstance() {
		return instance;
	}

	public void assign(Object bean, String property, Object value) {
		BonesOfBeans.setProperty(bean, property, value);
	}

	public void visitProperties(Object bean, PropertyVisitor visitor) {
		Map descriptors = BonesOfBeans.getAllPropertyDescriptors(bean);
		for (Iterator i = descriptors.values().iterator(); i.hasNext();) {
			PropertyDescriptor desc = (PropertyDescriptor) i.next();
			if (desc.getReadMethod() != null && desc.getWriteMethod() != null)
				visitor.onProperty(desc.getName(), desc.getPropertyType(),
						BonesOfBeans.getProperty(bean, desc));
		}
	}

	public HGAtomType getJavaBinding(HGHandle typeHandle, 
									 HGAtomType hgType, 
									 Class javaClass) 
	{
		if (hgType instanceof RecordType)
			return new JavaBeanBinding(typeHandle, (RecordType)hgType, javaClass);
		else if (hgType instanceof HGCompositeType)
			return new JavaAbstractBeanBinding(typeHandle, (HGCompositeType)hgType, javaClass);
		else if (hgType instanceof HGAbstractType)
			return new JavaInterfaceBinding(typeHandle, hgType, javaClass);
		else
			return hgType;
	}

	/**
	 * <p>
	 * Construct a new <code>HGAtomType</code> instance that will represent
	 * the Java <code>Class</code> passed in the <code>clazz</code>
	 * parameter. The new type must be further defined using the
	 * <code>defineHGType</code>. The construction of HG type representing
	 * Java types is thus split into two phases to avoid infinite recursion
	 * during the construction.
	 * </p>
	 */
	/* private HGAtomType constructHGType(HGTypeSystem typeSystem, Class clazz) {
		int modifiers = clazz.getModifiers();
		if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers)) {
			if (BonesOfBeans.getAllPropertyDescriptors(clazz).size() == 0)
				return new HGAbstractType();
			else
				return new HGAbstractCompositeType();
		}
		return new RecordType();
	} */

	/**
	 * <p>
	 * Determine and return the handle of the <code>HGAtomType</code>
	 * </p>
	 * 
	 * @param typeSystem
	 * @param javaClass
	 * @return
	 */
/*	public HGHandle getTypeConstructor(HGTypeSystem typeSystem, Class javaClass) {
		int modifiers = javaClass.getModifiers();
		if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers)) {
			if (BonesOfBeans.getAllPropertyDescriptors(javaClass).size() == 0)
				return typeSystem.getHandle(HGAbstractType.class);
			else
				return typeSystem.getHandle(HGAbstractCompositeType.class);
		} else
			return typeSystem.getHandle(RecordType.class);
	} */

	/**
	 * <p>
	 * Introspect a Java bean and create a hypergraph type for it, if it
	 * doesn't already exist. Auxiliary type information, such as record
	 * slots and the like, will be added to hypergraph as needed.
	 * </p>
	 */
	public HGAtomType defineHGType(HGTypeSystem typeSystem, Class javaClass, HGHandle typeHandle) 
	{
		if (javaClass == null)
			throw new NullPointerException(
					"JavaTypeFactory.getHGType: null beanClass parameter.");
		
		if (javaClass.isEnum())
			return new EnumType(javaClass);
		
		if (javaClass.isArray()) 
			return new ArrayType(javaClass.getComponentType());
		
		Map<String, PropertyDescriptor> descriptors = BonesOfBeans
				.getAllPropertyDescriptors(javaClass);
		boolean is_record = false;
		//
		// Determine whether the Java class has a "record" aspect to it: that
		// is,
		// whether there is at least one property that is both readable and
		// writeable.
		//
		for (PropertyDescriptor d : descriptors.values()) {
			if (d.getReadMethod() != null && d.getWriteMethod() != null) {
				is_record = true;
				break;
			}
		}

		boolean is_abstract = Modifier.isAbstract(javaClass.getModifiers())
				|| Modifier.isInterface(javaClass.getModifiers());

		boolean is_default_constructible = true;
		boolean is_link = false;		
		boolean is_serializable = java.io.Serializable.class.isAssignableFrom(javaClass);

		try 
		{
			javaClass.getDeclaredConstructor(new Class[] { HGHandle[].class });
			is_link = HGLink.class.isAssignableFrom(javaClass);
		} 
		catch (NoSuchMethodException ex) 
		{
		}

		try 
		{
			javaClass.getConstructor(new Class[0]);
		}
		catch (NoSuchMethodException ex) 
		{
			is_default_constructible = false;
		}

		is_record = is_record && (is_default_constructible || is_link);
		
		if (is_abstract) 
		{
			if (!is_record)
				return new HGAbstractType();
			else
				return defineComposite(typeSystem, descriptors);
		} 
		else if (Map.class.isAssignableFrom(javaClass)
				&& is_default_constructible) 
		{
			return new MapType(new GenericObjectFactory<Map<Object, Object>>(
					javaClass));
		} 
		else if (Collection.class.isAssignableFrom(javaClass)
				&& is_default_constructible) 
		{
			return new CollectionType(
					new GenericObjectFactory<Collection<Object>>(javaClass));
		}
		else if (is_record) 
		{
			RecordType recordType = new RecordType();
			HGHandle slotType = typeSystem.getTypeHandle(Slot.class);
			for (Iterator i = descriptors.values().iterator(); i.hasNext();) 
			{
				PropertyDescriptor desc = (PropertyDescriptor) i.next();

				//
				// Accept only properties that are both readable and writeable!
				//
				if (!includeProperty(javaClass, desc))
					continue;

				Slot slot = new Slot();
				slot.setLabel(desc.getName());
				
				Class propType = desc.getPropertyType();
				if (propType.isPrimitive())
					propType = BonesOfBeans.wrapperEquivalentOf(propType);
				HGHandle valueTypeHandle = typeSystem.getTypeHandle(propType);
				slot.setValueType(valueTypeHandle);
				AtomValueCondition cond = new AtomValueCondition(slot);
				HGQuery query = HGQuery.make(typeSystem.getHyperGraph(), cond);
				HGSearchResult<HGHandle> res = null;
				HGHandle slotHandle;
				try 
				{
					res = query.execute();
					if (!res.hasNext()) 
					{
						slotHandle = typeSystem.getHyperGraph().add(slot, slotType);
					} 
					else // the Slot is in the DB, but not in cache so put it
							// here
					{
						slotHandle = res.next();
						slot = (Slot) typeSystem.getHyperGraph().get(slotHandle);
					}
				} catch (Throwable t) {
					throw new HGException(t);
				} finally {
					if (res != null)
						res.close();
				}
				recordType.addSlot(slotHandle);

				HGAtomRef.Mode refMode = getReferenceMode(javaClass, desc);						
				if (refMode != null)
					typeSystem.getHyperGraph().add(new AtomProjection(typeHandle, slotHandle, refMode));
			}
			return recordType;
		}
		// Should we check for serializable here?
		// Doesn't seem right since being a bean and/or map and/or collection
		// doesn't exclude
		// a class from being serializable and having important state that is
		// simply not being
		// exposed.
		//
		// Another option is some sort of empty type. Instances don't hold any
		// persistent data, but
		// they exist and are typed nevertheless.
		//
		// Serialization is important for some standard classes that are not
		// default constructible, such
		// as the new (since Java 5) java.util.EnumMap.
		//     
		else if (is_serializable)
			return typeSystem.getAtomType(java.io.Serializable.class);
		else if (is_default_constructible || is_link) {
			// Nothing much more we can do...., perhaps some other default,
			// perhaps not.
			return new RecordType();
		} else
			return null;
	}

	private HGAtomType defineComposite(HGTypeSystem typeSystem,
			Map propertiesMap) {
		HGAbstractCompositeType compositeType = new HGAbstractCompositeType();
		for (Iterator i = propertiesMap.values().iterator(); i.hasNext();) {
			PropertyDescriptor desc = (PropertyDescriptor) i.next();

			//
			// Accept only properties that are both readable and writeable!
			//
			if (desc.getReadMethod() == null || desc.getWriteMethod() == null)
				continue;
			//
			// Retrieve or recursively create a new type for the nested bean.
			//	            
			Class propType = desc.getPropertyType();
			if (propType.isPrimitive())
				propType = BonesOfBeans.wrapperEquivalentOf(propType);
			compositeType.addProjection(new HGAbstractCompositeType.Projection(
					desc.getName(), typeSystem.getTypeHandle(propType)));
		}
		return compositeType;
	}
	
	private HGAtomRef.Mode getReferenceMode(Class javaClass, PropertyDescriptor desc)
	{
		//
		// Retrieve or recursively create a new type for the nested
		// bean.
		//
		try
		{
			Field field = javaClass.getDeclaredField(desc.getName());
			AtomReference ann = (AtomReference)field.getAnnotation(AtomReference.class);
			if (ann == null)
				return null;
			String s = ann.value();
			if ("hard".equals(s))
				return HGAtomRef.Mode.hard;
			else if ("symbolic".equals(s))
				return HGAtomRef.Mode.symbolic;
			else if ("floating".equals(s))
				return HGAtomRef.Mode.floating;
			else
				throw new HGException("Wrong annotation value '" + s + 
						"' for field '" + field.getName() + "' of class '" +
						javaClass.getName() + "', must be one of \"hard\", \"symbolic\" or \"floating\".");
		}
		catch (NoSuchFieldException ex)
		{
			// Perhaps issue a warning here if people are misspelling
			// unintentionally? Proper spelling is only useful for
			// annotation, so a warning/error should be really issued if
			// we find an annotation for a field that we can't make
			// use of?
			return null;
		}		
	}
	
	private boolean includeProperty(Class javaClass, PropertyDescriptor desc)
	{
		Method reader = desc.getReadMethod();
		Method writer = desc.getWriteMethod();
		if (reader == null || writer == null)
			return false;
		if (reader.getAnnotation(HGIgnore.class) != null ||
			writer.getAnnotation(HGIgnore.class) != null)
			return false;
		try
		{
			Field field = javaClass.getDeclaredField(desc.getName());
			return field.getAnnotation(HGIgnore.class) == null;
		}
		catch (NoSuchFieldException ex)
		{
			return true;
		}
	}
}