package org.hypergraphdb.type;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.type.javaprimitive.EnumType;

public class DefaultJavaTypeMapper implements JavaTypeMapper
{
	private HyperGraph graph;
	
	private HGAtomType defineComposite(HGTypeSystem typeSystem, Map<String, PropertyDescriptor> propertiesMap) 
	{
		HGAbstractCompositeType compositeType = new HGAbstractCompositeType();
		for (Iterator<PropertyDescriptor> i = propertiesMap.values().iterator(); i.hasNext();) {
			PropertyDescriptor desc = i.next();

			//
			// Accept only properties that are both readable and writeable!
			//
			if (desc.getReadMethod() == null || desc.getWriteMethod() == null)
				continue;
			//
			// Retrieve or recursively create a new type for the nested bean.
			//	            
			Class<?> propType = desc.getPropertyType();
			if (propType.isPrimitive())
				propType = BonesOfBeans.wrapperEquivalentOf(propType);
			compositeType.addProjection(new HGAbstractCompositeType.Projection(
					desc.getName(), typeSystem.getTypeHandle(propType)));
		}
		return compositeType;
	}
	
	private HGAtomRef.Mode getReferenceMode(Class<?> javaClass, PropertyDescriptor desc)
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
	
	private boolean includeProperty(Class<?> javaClass, PropertyDescriptor desc)
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
	
	@SuppressWarnings("unchecked")
	public HGAtomType defineHGType(Class<?> javaClass, HGHandle typeHandle)
	{
		HGTypeSystem typeSystem = graph.getTypeSystem();
		JavaTypeFactory javaTypes = typeSystem.getJavaTypeFactory();
		
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

		boolean is_abstract = javaTypes.isAbstract(javaClass);
		boolean is_default_constructible = javaTypes.isDefaultConstructible(javaClass);
		boolean is_link = javaTypes.isLink(javaClass);
		boolean is_serializable = java.io.Serializable.class.isAssignableFrom(javaClass);

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
					(Class<Map<Object, Object>>)javaClass));
		} 
		else if (Collection.class.isAssignableFrom(javaClass)
				&& is_default_constructible) 
		{
			return new CollectionType(
					new GenericObjectFactory<Collection<Object>>((Class<Collection<Object>>)javaClass));
		}
		else if (is_record) 
		{
			RecordType recordType = new RecordType();
			for (Iterator<PropertyDescriptor> i = descriptors.values().iterator(); i.hasNext();) 
			{
				PropertyDescriptor desc = i.next();

				//
				// Accept only properties that are both readable and writeable!
				//
				if (!includeProperty(javaClass, desc))
					continue;

				Class<?> propType = desc.getPropertyType();
				if (propType.isPrimitive())
					propType = BonesOfBeans.wrapperEquivalentOf(propType);
				HGHandle valueTypeHandle = typeSystem.getTypeHandle(propType);
				HGHandle slotHandle = javaTypes.getSlotHandle(desc.getName(), 
															  valueTypeHandle);
				recordType.addSlot(slotHandle);
				HGAtomRef.Mode refMode = getReferenceMode(javaClass, desc);						
				if (refMode != null)
					typeSystem.getHyperGraph().add(new AtomProjection(typeHandle, 
																	  slotHandle, 
																	  refMode));
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

	public HGAtomType getJavaBinding(HGHandle typeHandle, 
									 HGAtomType hgType, 
									 Class<?> javaClass)
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
	
	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}	
}
