/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.event.HGLoadPredefinedTypeEvent;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.JavaTypeFactory;
import org.hypergraphdb.type.LinkType;
import org.hypergraphdb.type.PlainLinkType;
import org.hypergraphdb.type.SubsumesType;
import org.hypergraphdb.type.Top;

/**
 * <p>
 * The <code>HGTypeSystem</code> manages atom type information for a given
 * hypergraph database. Every hypergraph database can have its own user-definable
 * type system.
 * </p>
 *
 *
 * <h3>Aliases</h3>
 *
 * <p>
 * An alias can be defined for a commonly used type. An alias is simply a name
 * that is associated with a type. The type instance can then be retrieved by
 * using the alias. A type may have more than one alias. Use the
 * <code>addAlias</code>, <code>removeAlias</code>, <code>getType(String)</code>
 * and <code>getHandle</code> methods for working with aliases.
 * </p>
 *
 * @author Borislav Iordanov
 */
public class HGTypeSystem
{
	private static final String TYPE_ALIASES_DB_NAME = "hg_typesystem_type_alias";
	private static final String JAVA2HG_TYPES_DB_NAME = "hg_typesystem_java2hg_types";
	private static final String JAVA_PREDEFINED_TYPES_DB_NAME = "hg_typesystem_javapredefined_types";
	private static final int MAX_CLASS_TO_TYPE = 2000;

	public static final HGPersistentHandle TOP_PERSISTENT_HANDLE =
		HGHandleFactory.makeHandle("a395bb09-07cd-11da-831d-8d375c1471fe");

	public static final HGPersistentHandle LINK_PERSISTENT_HANDLE =
		HGHandleFactory.makeHandle("822fa5aa-a376-11da-9470-a5016df901a7");

	public static final HGPersistentHandle PLAINLINK_PERSISTENT_HANDLE =
		HGHandleFactory.makeHandle("8c8202fb-a376-11da-9470-a5016df901a7");

	public static final HGPersistentHandle SUBSUMES_PERSISTENT_HANDLE =
		HGHandleFactory.makeHandle("971a1bdc-a376-11da-9470-a5016df901a7");

	public static final HGPersistentHandle NULLTYPE_PERSISTENT_HANDLE =
		HGHandleFactory.makeHandle("db733325-19d5-11db-8b55-23bc8177d6ec");

	public static final HGAtomType top = Top.getInstance();

	private HyperGraph hg = null;
	private Map<Class<?>, HGHandle> classToAtomType = Collections.synchronizedMap(new ClassToTypeCache());
	private HGBidirectionalIndex<String, HGPersistentHandle> classToTypeDB = null;
	private HGBidirectionalIndex<String,  HGPersistentHandle> aliases = null;
	private HGIndex<HGPersistentHandle, String> predefinedTypesDB = null;
	private JavaTypeFactory javaTypes = null;
	
	private HGBidirectionalIndex<String, HGPersistentHandle> getClassToTypeDB()
	{
		if (classToTypeDB == null)
		{
			classToTypeDB = hg.getStore().getBidirectionalIndex(JAVA2HG_TYPES_DB_NAME,
			                                       BAtoString.getInstance(),
			                                       BAtoHandle.getInstance(),
			                                       null);
			if (classToTypeDB == null)
				classToTypeDB = hg.getStore().createBidirectionalIndex(JAVA2HG_TYPES_DB_NAME,
				                                          BAtoString.getInstance(),
				                                          BAtoHandle.getInstance(),
				                                          null);
		}
		return classToTypeDB;
	}

	private HGBidirectionalIndex<String, HGPersistentHandle> getAliases()
	{
		if (aliases == null)
		{
			aliases = hg.getStore().getBidirectionalIndex(TYPE_ALIASES_DB_NAME,
			                                   BAtoString.getInstance(),
			                                   BAtoHandle.getInstance(),
			                                   null);
			if (aliases == null)
				aliases = hg.getStore().createBidirectionalIndex(TYPE_ALIASES_DB_NAME,
				                                     BAtoString.getInstance(),
				                                     BAtoHandle.getInstance(),
				                                     null);
		}
		return aliases;
	}

	private HGIndex<HGPersistentHandle, String> getPredefinedTypesDB()
	{
		if (predefinedTypesDB == null)
		{
			predefinedTypesDB = hg.getStore().getIndex(JAVA_PREDEFINED_TYPES_DB_NAME,
			                                 BAtoHandle.getInstance(),
			                                 BAtoString.getInstance(),
			                                 null);
			if (predefinedTypesDB == null)
				predefinedTypesDB = hg.getStore().createIndex(JAVA_PREDEFINED_TYPES_DB_NAME,
				                                BAtoHandle.getInstance(),
				                                BAtoString.getInstance(),
				                                null);
		}
		return predefinedTypesDB;
	}

	void addPrimitiveTypeToStore(HGPersistentHandle handle)
	{
		HGPersistentHandle [] layout = new HGPersistentHandle[]
			{
				TOP_PERSISTENT_HANDLE,
				HGHandleFactory.nullHandle()
			};
		hg.getStore().store(handle, layout);
//		if (hg.indexByType.)
		hg.indexByType.addEntry(TOP_PERSISTENT_HANDLE, handle);
	}

	void bootstrap(String typeDefResource)
	{
		top.setHyperGraph(hg);
		HGLiveHandle topHandle = hg.cache.atomRead(TOP_PERSISTENT_HANDLE, top, (byte)HGSystemFlags.DEFAULT);
		classToAtomType.put(Top.class, topHandle); // TOP is its own type
		classToAtomType.put(Object.class, topHandle); // TOP also corresponds to the java.lang.Object "top type"
		hg.cache.freeze(topHandle);

		HGAtomType plainLinktype = new PlainLinkType();
		plainLinktype.setHyperGraph(hg);
		HGLiveHandle plainLinkHandle = hg.cache.atomRead(PLAINLINK_PERSISTENT_HANDLE, plainLinktype, (byte)HGSystemFlags.DEFAULT);
		classToAtomType.put(HGPlainLink.class, plainLinkHandle);
		hg.cache.freeze(plainLinkHandle);

		HGAtomType linkType = new LinkType();
		linkType.setHyperGraph(hg);
		HGLiveHandle linkHandle = hg.cache.atomRead(LINK_PERSISTENT_HANDLE, linkType, (byte)HGSystemFlags.DEFAULT);
		classToAtomType.put(HGLink.class, linkHandle);
		hg.cache.freeze(linkHandle);

		HGAtomType subsumesType = new SubsumesType();
		subsumesType.setHyperGraph(hg);
		HGLiveHandle subsumesHandle = hg.cache.atomRead(SUBSUMES_PERSISTENT_HANDLE, subsumesType, (byte)HGSystemFlags.DEFAULT);
		classToAtomType.put(HGSubsumes.class, subsumesHandle);
		hg.cache.freeze(subsumesHandle);

		//
		// If we are actually creating a new database, populate with primitive types.
		//
		if (hg.getStore().getLink(TOP_PERSISTENT_HANDLE) == null)
		{
			addPrimitiveTypeToStore(TOP_PERSISTENT_HANDLE);
			addPrimitiveTypeToStore(LINK_PERSISTENT_HANDLE);
			addPrimitiveTypeToStore(PLAINLINK_PERSISTENT_HANDLE);
			addPrimitiveTypeToStore(SUBSUMES_PERSISTENT_HANDLE);
			hg.add(new HGSubsumes(TOP_PERSISTENT_HANDLE, LINK_PERSISTENT_HANDLE), SUBSUMES_PERSISTENT_HANDLE);
			hg.add(new HGSubsumes(LINK_PERSISTENT_HANDLE, PLAINLINK_PERSISTENT_HANDLE), SUBSUMES_PERSISTENT_HANDLE);
			hg.add(new HGSubsumes(PLAINLINK_PERSISTENT_HANDLE, SUBSUMES_PERSISTENT_HANDLE), SUBSUMES_PERSISTENT_HANDLE);
			storePrimitiveTypes(typeDefResource);
		}
	}
	
	/**
	 * <p>
	 * Use this method to load a set of primitive types in bulk, from a text descriptor
	 * resource (loaded using this class's class loader).
	 * </p>
	 * <p>
	 * The resource is expected to be in the following format: 1 type per line where
	 * each line consists of two or more columns separated by the space character.
	 * The first columns should be a canonical string representation of a UUID. The second
	 * column should be the classname of the class implementing the type. The (optional)
	 * subsequent columns should list the names of the classes that this type "covers".
	 * The following is an example where the first line simply adds a predefined type
	 * without any corresponding covered Java classes, and the second shows a type
	 * that covers only one class:
	 * </p>
	 * <p>
	 * <pre><code>
	 * db733325-19d5-11db-8b55-23bc8177d6ec org.hypergraphdb.type.NullType
	 * 2ec10476-d964-11db-a08c-eb6f4c8f155a org.hypergraphdb.type.AtomRefType org.hypergraphdb.atom.HGAtomRef
	 * </code></pre>
	 * </p>
	 * @param resource
	 */
	public void storePrimitiveTypes(String resource)
	{
	   InputStream resourceIn = getClass().getResourceAsStream(resource);
	   if (resourceIn == null)
		   throw new HGException("Fatal error: could not load primitive types from " +
			                      resource + ", this resource could not be found!");
	   BufferedReader reader = new BufferedReader(new InputStreamReader(resourceIn));
	   try
	   {
		   for (String line = reader.readLine(); line != null; line = reader.readLine())
		   {
			   line = line.trim();
			   if (line.length() == 0)
				   continue;

			   if (line.startsWith("#"))
				   continue;

			   StringTokenizer tok = new StringTokenizer(line, " ");
			   if (tok.countTokens() < 2)
				   throw new HGException("Fatal error: could not load primitive types from " +
					                      resource + ", the line " + line + " is ill formed.");
			   String pHandleStr = tok.nextToken();
			   String typeClassName = tok.nextToken();
			   Class<?> typeClass = Class.forName(typeClassName);
			   HGAtomType type = (HGAtomType)typeClass.newInstance();
			   type.setHyperGraph(hg);
			   HGPersistentHandle pHandle = HGHandleFactory.makeHandle(pHandleStr);
			   if (tok.hasMoreTokens()) {
				   while (tok.hasMoreTokens())
				   {
					   String valueClassName = tok.nextToken();
					   Class<?> valueClass = Class.forName(valueClassName);
					   addPredefinedType(pHandle, type, valueClass);
				   }
				}
			   else
				   addPredefinedType(pHandle, type, null);
		   }
	   }
	   catch (IOException ex)
	   {
		   throw new HGException("Fatal error: could not load primitive types from " +
			                       resource + " due to an IO exception!", ex);
	   }
	   catch (ClassNotFoundException ex)
	   {
		   throw new HGException("Fatal error: could not load primitive types from " +
			                       resource + " due to a missing class from the classpath: " + ex.getMessage(), ex);
	   }
	   catch (Throwable t)
	   {
		   throw new HGException("Fatal error: could not load primitive types from " + resource, t);
	   }
	}

	HGLiveHandle loadPredefinedType(HGPersistentHandle pHandle)
	{
		hg.getEventManager().dispatch(hg, new HGLoadPredefinedTypeEvent(pHandle));
		HGLiveHandle result = hg.cache.get(pHandle);
		if (result != null)
			return result;
		else
		{
			String classname = getPredefinedTypesDB().findFirst(pHandle);
			if (classname == null)
			{
				throw new HGException("Unable to load predefined type with handle " +
				           pHandle +
			              " please review the documentation about predefined types and how to hook them with the HyperGraph type system.");
			}
			try
			{
				Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(classname);
				HGAtomType type = (HGAtomType)clazz.newInstance();
				type.setHyperGraph(hg);
				return (HGLiveHandle)addPredefinedType(pHandle, type, null);
			}
			catch (Throwable ex)
			{
				throw new HGException("Could not create predefined type instance with " +
				                    classname + " for type " + pHandle + ": " + ex.toString(), ex);
			}
		}
	}

	/**
	 * <p>
	 * Create a HyperGraph type for the specified Java class and store the type
	 * under the passed in <code>handle</code>. 
	 * </p>
	 *
	 * @param handle
	 * @param clazz
	 */
	public void defineTypeAtom(final HGPersistentHandle handle, final Class<?> clazz)
	{
		if (hg.getTransactionManager().getContext().getCurrent() != null)
		{
			hg.define(handle, NULLTYPE_PERSISTENT_HANDLE, NULLTYPE_PERSISTENT_HANDLE, null);
			classToAtomType.put(clazz, handle);
			getClassToTypeDB().addEntry(clazz.getName(), hg.getPersistentHandle(handle));
			HGHandle h = defineNewJavaTypeTransaction(handle, clazz);
			if (h == null)
				throw new HGException("Could not create HyperGraph type for class '" + clazz.getName() + "'");				
			else if (!h.equals(handle))
				throw new HGException("The class '" + clazz.getName() + "' already has a HyperGraph Java:"+h);

		}
		else
			hg.getTransactionManager().transact(new Callable<HGHandle>() {
			public HGHandle call()
			{
				hg.define(handle, NULLTYPE_PERSISTENT_HANDLE, NULLTYPE_PERSISTENT_HANDLE, null);
				classToAtomType.put(clazz, handle);
				getClassToTypeDB().addEntry(clazz.getName(), hg.getPersistentHandle(handle));
				HGHandle h = defineNewJavaTypeTransaction(handle, clazz);
				if (h == null)
					throw new HGException("Could not create HyperGraph type for class '" + clazz.getName() + "'");					
				else if (!h.equals(handle))
					throw new HGException("The class '" + clazz.getName() + "' already has a HyperGraph Java:"+h);
				else
					return h;
			} });
	}

	HGHandle defineNewJavaType(final Class<?> clazz)
	{
		try
		{
			if (hg.getTransactionManager().getContext().getCurrent() != null)
				return makeNewJavaType(clazz);
			else
				return hg.getTransactionManager().transact(new Callable<HGHandle>()
						{ public HGHandle call() { return makeNewJavaType(clazz); } });
		}
		catch (RuntimeException t)
		{
			classToAtomType.remove(clazz);
			throw t;
		}
	}

	HGHandle makeNewJavaType(Class<?> clazz)
	{
		//
		// First, create a dummy type for the class, so that recursive type
		// references don't lead to an infinite recursion here.
		//
		HGHandle newHandle = hg.add(clazz, NULLTYPE_PERSISTENT_HANDLE);
		classToAtomType.put(clazz, newHandle);
		getClassToTypeDB().addEntry(clazz.getName(), hg.getPersistentHandle(newHandle));
		HGHandle inferred = defineNewJavaTypeTransaction(newHandle, clazz);
		if (inferred == null)
		{
			// rollback changes
			getClassToTypeDB().removeAllEntries(clazz.getName());
			classToAtomType.remove(clazz);
			hg.remove(newHandle);
			return null;
		}
		else
			return inferred;
	}

	/**
	 * We need to infer to HG type by introspection. We maintain the
	 * full inheritence tree of Java class and interfaces. Therefore, for each
	 * newly added Java type mapping, we navigate to parent classes etc.
	 */
	HGHandle defineNewJavaTypeTransaction(HGHandle newHandle, Class<?> clazz)
	{
		//
		// Next, create a HyperGraph type matching the Java class.
		//
		HGAtomType inferredHGType = javaTypes.defineHGType(clazz, newHandle);

		if (inferredHGType == null)
			return null;
			// throw new HGException("Could not create HyperGraph type for class '" + clazz.getName() + "'");

		//
		// Now, replace the dummy atom that we added at the beginning with the new type.
		//
		HGHandle typeConstructor = getTypeHandle(inferredHGType.getClass());
		if (!typeConstructor.equals(TOP_PERSISTENT_HANDLE))
			hg.replace(newHandle, inferredHGType, typeConstructor);
		//
		// TODO: we are assuming here that if the defineHGType call above did not return
		// a newly created type, but an already existing one, its type constructor can
		// only be Top. There is no reason this should be true in general. Probably
		// defineHGType should return multiple values: the resulting type, whether it's new
		// or not and possibly other things, encapsulated in some sort of type descriptor.
		//
		else
		{
			// We have a predefined type. We must erase the dummy atom instead of replacing it.
			hg.remove(newHandle);

			// A predefined type is a frozen atom in the cache, so its live handle is available
			// from the Java instance.
			HGHandle result = hg.cache.get(inferredHGType);

			//
			// Update the Class -> type handle mapping.
			//
			classToAtomType.put(clazz, result);
			getClassToTypeDB().removeEntry(clazz.getName(), hg.getPersistentHandle(newHandle));
			getClassToTypeDB().addEntry(clazz.getName(), hg.getPersistentHandle(result));
			newHandle = result;
		}

		//
		// So far, the inferredHGType is platform independent HyperGraph type, e.g.
		// a RecordType. To make it transparently handle run-time instances of 'clazz',
		// we need a corresponding Java binding for this type, e.g. a JavaBeanBinding.
		//
		HGAtomType type = javaTypes.getJavaBinding(newHandle, inferredHGType, clazz);
		type.setHyperGraph(hg);

		//
		// the result of hg.add may or may not be stored in the cache: if it is, replace the run-time
		// instance of
		//
		if (newHandle instanceof HGLiveHandle)
			hg.cache.atomRefresh((HGLiveHandle)newHandle, type);
		else
			newHandle = hg.cache.atomRead((HGPersistentHandle)newHandle, type, (byte)HGSystemFlags.DEFAULT);

		// Now, examine the super type and implemented interfaces
		// First, make sure we've mapped all interfaces
		Class<?> [] interfaces = clazz.getInterfaces();
		for (int i = 0; i < interfaces.length; i++)
		{
			HGHandle interfaceHandle = getTypeHandle(interfaces[i]);
			if (interfaceHandle == null)
				throw new HGException("Unable to infer HG type for interface " +
				                       interfaces[i].getName());
			else
				hg.add(new HGSubsumes(interfaceHandle, newHandle));
		}
		//
		// Next, navigate to the superclass.
		//
		if (clazz.getSuperclass() != null)
		{
			HGHandle superHandle = getTypeHandle(clazz.getSuperclass());
			// Then proceed recursively to the superclass:
			if (superHandle == null)
			{
				throw new HGException("Unable to infer HG type for class " +
				                      clazz.getSuperclass().getName() +
				                      " the superclass of " + clazz.getName());
			}
			else
				hg.add(new HGSubsumes(superHandle, newHandle));
		}
		// Interfaces don't derive from java.lang.Object, so we need to super-type them with Top explicitely
		else if (clazz.isInterface())
			hg.add(new HGSubsumes(TOP_PERSISTENT_HANDLE, newHandle));

		//
		// ouf, we're done
		//
		return newHandle;
	}

	/**
	 * <p>Construct the <code>HGtypeSystem</code> associated with a hypergraph.</p>
	 *
	 * @param hg The <code>HyperGraph</code> which the type system is bound.
	 */
	public HGTypeSystem(HyperGraph hg)
	{
		this.hg = hg;
		//
		// Initialize databases to avoid heaving to synchronize later.
		//
		this.getAliases();
		this.getClassToTypeDB();
		this.getPredefinedTypesDB();
		this.javaTypes = new JavaTypeFactory();
		javaTypes.setHyperGraph(hg);
	}

	/**
	 * <p>Return the <code>HyperGraph</code> on which this type
	 * system operates.
	 * </p>
	 */
	public HyperGraph getHyperGraph()
	{
		return hg;
	}

	/** 
	 * <p>Return the <code>JavaTypeFactory</code> which is responsible for mapping
	 * Java class to HyperGraph types.</p>
	 */
	public JavaTypeFactory getJavaTypeFactory()
	{
		return this.javaTypes;
	}
	
	public HGAtomType getTop()
	{
		return top;
	}

	Class<?> loadClass(String classname)
	{
		Class<?> clazz;
		try
		{
			if(classname.startsWith("[L"))
			{
				classname = classname.substring(2, classname.length() - 1); //remove ending ";"
				clazz = Array.newInstance(Thread.currentThread().getContextClassLoader().loadClass(classname), 0).getClass();
			}
			else
			   clazz = Thread.currentThread().getContextClassLoader().loadClass(classname);
			return clazz;
		}
		catch (Throwable t)
		{
			throw new HGException("Could not load class " + classname, t);
		}
	}
	
	/**
	 * <p>HyperGraph internal method to handle the loading of a type. A type can be
	 * loaded in one of two ways: either through the <code>getType(HGHandle)</code>
	 * of this class or directly by calling <code>HyperGraph.get</code>. In both cases,
	 * the type system should be explicitely made aware that a new type has been loaded
	 * and be given the possibility to decorate the HGAtomType instance....
	 * </p>
	 *
	 * @param type
	 */
	HGAtomType loadedType(HGLiveHandle handle, HGAtomType type, boolean refreshInCache)
	{
		String classname = getClassToTypeDB().findFirstByValue(handle.getPersistentHandle());
		if (classname != null)
		{		  
			Class<?> clazz = loadClass(classname);
			type = javaTypes.getJavaBinding(handle, type, clazz);
			if (refreshInCache)
			{
				hg.cache.atomRefresh(handle, type);
				classToAtomType.put(clazz, handle);
			}
		}
		return type;
	}
	
	/**
	 * <p>Specify an application specific predefined type, possibly overriding a default
	 * HyperGraph basic type. This method allows you to add base level types when the primitive types
	 * and type constructors provided with HyperGraph are not sufficient. For instance, one may
	 * replace the handling of simple data types such as strings and booleans, or the management
	 * of certain structured data such as a particular Java class etc.</p>
	 *
	 * <p>
	 * Any <code>HGAtomType</code> that does not have a proper representation in the HyperGraph storage,
	 * should be added at application startup time through this method. While generally it will,
	 * such a top-level type does not need to correspond to a Java type. If there's no corresponding
	 * Java type, the <code>clazz</code> parameter in a call to this method should be <code>null</code>.
	 * </p>
	 *
	 * <p>
	 * Note that a HyperGraph type may map to more than one corresponding Java class. Thus, multiple
	 * calls with the same <code>type</code> parameter, but different <code>clazz</code> parameters
	 * can be made to create a many-to-one relationship between Java type and HyperGraph types.
	 * </p>
	 *
	 * <p>
	 * There is one special in the mapping of HyperGraph types to Java types: the handling of Java
	 * primitive arrays. From HyperGraph's storage perspective, all arrays are generally recorded in the same
	 * way regardless of the type of their elements (each element is stored through its own type).
	 * Therefore, a single HyperGraph array type would be able to handle all Java primitive arrays.
	 * Of course, it is possible to have specific implementations for a particular <code>T[]</code>
	 * Java types (for instance, an optimized <code>boolean[]</code>). But there is a special generic
	 * handling of all Java built-in arrays that is specified as the HyperGraph type of the
	 * <code>Object[]</code> class. That is, to specify the <code>HGAtomType</code>
	 * that should be used for Java primitive array storage, use the class of <code>Object[]</code>
	 * as the third parameter of this method. For example:
	 * </p>
	 *
	 * <p><code>
	 * typeSystem.addPredefinedType(persistentHandle, type, Class.forName("[Ljava.lang.Object;"));
	 * </code></p>
	 *
	 * @param handle The persistent handle of this type.
	 * @param type The run-time instance of the type.
	 * @param clazz The Java class to which this type corresponds. All atoms that are instances
	 * of this Java class will be handled through this type. This parameter may be null if the
	 * type should not be mapped to a Java class.
	 * @return A run-time handle for the newly added type.
	 */
	public HGHandle addPredefinedType(final HGPersistentHandle handle, final HGAtomType type, final Class<?> clazz)
	{
		if (hg.getTransactionManager().getContext().getCurrent() != null)
			return addPredefinedTypeTransaction(handle, type, clazz);
		else
			return hg.getTransactionManager().transact(new Callable<HGHandle>()
				{ public HGHandle call() { return addPredefinedTypeTransaction(handle, type, clazz); } });
	}
	private HGHandle addPredefinedTypeTransaction(HGPersistentHandle handle, HGAtomType type, Class<?> clazz)
	{
		//
		// Make sure the type is in storage...
		//
		if (hg.getStore().getLink(handle) == null)
		{
			addPrimitiveTypeToStore(handle);
			hg.add(new HGSubsumes(TOP_PERSISTENT_HANDLE, handle), SUBSUMES_PERSISTENT_HANDLE);
			try
			{
				//
				// If the type class has a default constructor, we add it to the list
				// automatically instantiable predefined types. Otherwise, we can't
				// instantiate it, and it is somebody else's business to do so.
				//
				if (type.getClass().getConstructor(new Class[0]) != null)
					getPredefinedTypesDB().addEntry(handle, type.getClass().getName());
			}
			catch (NoSuchMethodException e) { /* Log this some day when we have logging. */}
		}

		HGLiveHandle typeHandle = hg.cache.atomRead(handle, type, (byte)HGSystemFlags.DEFAULT);
		hg.cache.freeze(typeHandle);
		classToAtomType.put(type.getClass(), classToAtomType.get(Top.class));
		if (clazz != null)
		{
			if (getClassToTypeDB().findFirst(clazz.getName()) == null)
				classToTypeDB.addEntry(clazz.getName(), handle);
			classToAtomType.put(clazz, typeHandle);
//			if (clazz.equals((new Object[0]).getClass()))
//				this.primitiveArrayType = handle;
		}
		return typeHandle;
	}

	/**
	 * <p>
	 * Return the Java class that corresponds to the given HyperGraphDB type handle. The
	 * result is the class of the run-time instances constructed with the type identified
	 * by <code>typeHandle</code>. 
	 * </p>
	 * 
	 * @param typeHandle The <code>HGHandle</code> identifying the type whose runtime Java
	 * class is required.
	 * @return The Java class corresponding to <code>typeHandle</code> or <code>null</code>
	 * if there's no such correspondence.
	 */
	public Class<?> getClassForType(HGHandle typeHandle)
	{
		String classname = getClassToTypeDB().findFirstByValue(hg.getPersistentHandle(typeHandle));
		return classname != null ? loadClass(classname) : null;
	}
	
	/**
	 * <p>Return the <code>HGAtomType</code> by its <code>HGHandle</code>.</p>
	 *
	 * @param handle The handle of the atom type itself. Note that to retrieve the type
	 * of an atom, you must use the <code>getAtomType(Object)</code> method.
	 */
	public HGAtomType getType(HGHandle handle)
	{
		return (HGAtomType)hg.get(handle);
	}

	/**
	 * <p>Return the <code>HGAtomType</code> corresponding to the given alias.</p>
	 *
	 * @param alias The alias.
	 * @return The type instance or <code>null</code> if this alias has not
	 * been defined.
	 */
	public HGAtomType getType(String alias)
	{
		HGHandle handle = getTypeHandle(alias);
		if (handle != null)
			return getType(handle);
		else
			return null;
	}

	/**
	 * <p>
	 * Return the <code>HGAtomType</code> corresponding to the passed in
	 * Java class. This is equivalent to <code>(HGAtomType)HyperGraph.get(getTypeHandle(clazz))</code>.
	 * </p>
	 *
	 */
	public HGAtomType getAtomType(Class<?> clazz)
	{
		return (HGAtomType)hg.get(getTypeHandle(clazz));
	}

	/**
	 * <p>
	 * Return the default <code>HyperGraph</code> type of the given atom object. Note
	 * the <em>default</em> here means that the type returned is the one that would
	 * be automatically assigned to instances of the concrete type of <code>object</code>.
	 * That is, calling this method is equivalent to calling <code>getAtomType(object.getClass())</code>.
	 * If <code>object</code> is the run-time instance of an actual HyperGraph atom that
	 * was explicitely assigned a type, the latter may be different than the default type.
	 * </p>
	 */
	public HGAtomType getAtomType(Object object)
	{
		return getType(getTypeHandle(object));
	}

	/**
	 * <p>Return the type instance of a given atom.</p>
	 *
	 * @param handle The atom whose type is desired.
	 * @return The type of the atom.
	 */
	public HGAtomType getAtomType(HGHandle handle)
	{
		return (HGAtomType)getTypeHandle(handle);
	}

	/**
	 * <p>Return <code>true</code> if there is a HyperGraph type corresponding to the given
	 * class and <code>false</code> otherwise.</p>
	 */
	public boolean hasType(Class<?> clazz)
	{
		if (classToAtomType.containsKey(clazz))
			return true;
		else if (getClassToTypeDB().findFirst(clazz.getName()) != null)
			return true;
		else
			return false;
	}

	/**
	 * <p>Return the <code>HGHandle</code> of the HyperGraph type representing a given
	 * Java class. If no type has been associated yet with that particular <code>Class</code>, a
	 * new one will be created using the currently active <code>JavaTypeFactory</code>.</p>
	 *
	 * @param clazz The <code>Class</code> instance of the Java class. Cannot be <code>null</code>
	 * @return The <code>HGHandle</code> for that class. If the Java class hasn't been previously
	 * mapped to a HyperGraph atom type, a new HyperGraph type will be created and the new handle
	 * will be returned.
	 */
	public HGHandle getTypeHandle(Class<?> clazz)
	{
		if (clazz.isPrimitive())
			clazz = BonesOfBeans.wrapperEquivalentOf(clazz);

		//
		// First check cache of class to HG type mappings:
		//
		HGHandle typeHandle = classToAtomType.get(clazz);
		if (typeHandle != null)
			return typeHandle;

		//
		// Then check the database of inferred types:
		//
		HGPersistentHandle hgTypeHandle = getClassToTypeDB().findFirst(clazz.getName());
		if (hgTypeHandle != null)
			return hgTypeHandle;

		//
		// No HG type defined specifically for this concrete clazz. First check
		// the "built in" array case.
		//
		if (clazz.isArray())
		{
			Class<?> clazz1 = (new Object[0]).getClass();
			typeHandle = classToAtomType.get(clazz1);
			if (typeHandle != null)
				return typeHandle;
			hgTypeHandle = getClassToTypeDB().findFirst(clazz1.getName());
			if (hgTypeHandle != null)
				return hgTypeHandle;
			 return defineNewJavaType(clazz);
			//throw new HGException("Could not handle array type for " + clazz.getComponentType().getName() +
			//		" since there is no HyperGraph neither for this array type, nor for the generic Object[].");
		}

		return defineNewJavaType(clazz);
	}

	/**
	 * <p>Return the handle of the type corresponding to the given alias.</p>
	 *
	 * @param alias The alias.
	 * @return The type handle or <code>null</code> if this alias has not
	 * been defined.
	 */
	public HGHandle getTypeHandle(String alias)
	{
		if (alias == null)
			return null;
		else
		{
			HGPersistentHandle handle = getAliases().findFirst(alias);
			if (handle != null)
				return hg.refreshHandle(handle);
			else
				return null;
		}
	}

	public HGHandle getTypeHandle(HGHandle atomHandle)
	{
		HGPersistentHandle [] layout = hg.getStore().getLink(hg.getPersistentHandle(atomHandle));
		if (layout == null || layout.length == 0)
			throw new HGException("Could not retrieve atom with handle " +
			                      hg.getPersistentHandle(atomHandle) + " from the HyperGraph store.");
		HGHandle live = hg.cache.get(layout[0]);
		return live == null ? layout[0] : live;
	}

	/**
	 * <p>
	 * Return the HyperGraph type handle of the given Java object.
	 * </p>
	 *
	 * <p>
	 * This method will first try to find the HyperGraph <code>HGHandle</code> of the object
	 * and retrieve the type based on that handle. If not, it will retrieve the default
	 * HyperGraph type of the concrete Java class of the object (i.e. of x.getClass()).
	 * </p>
	 *
	 * @param x The object whose HyperGraph type is desired. Cannot be <code>null</code>.
	 * @return The <code>HGHandle</code> of the HyperGraph type for that object.
	 */
	public HGHandle getTypeHandle(Object x)
	{
		if (x == null)
			throw new NullPointerException(
			        "HGTypeSystem.getAtomType(Object) invoked with a null object -- and 'null' has no type.");
		HGHandle atom = hg.getHandle(x);
		if (atom != null)
			return getTypeHandle(atom);
		else
			return getTypeHandle(x.getClass());
	}

	/**
	 * <p>
	 * Add a new alias for a given type.
	 * </p>
	 *
	 * @param typeHandle The <code>HGPersistentHandle</code> of the type. Note
	 * that the method doesn't check whether this is in fact a type handle.
	 * @param alias A non-null alias name. If this name is already used to alias
	 * another type, an exception is thrown.
	 */
	public void addAlias(final HGHandle typeHandle, final String alias)
	{
		if (hg.getTransactionManager().getContext().getCurrent() != null)
			addAliasTransaction(typeHandle, alias);
		else
			hg.getTransactionManager().transact(new Callable<Object>()
				{ public Object call() { addAliasTransaction(typeHandle, alias); return null; } });
	}
	private void addAliasTransaction(HGHandle typeHandle, String alias)
	{
		HGBidirectionalIndex<String, HGPersistentHandle> aliases = getAliases();
		synchronized (aliases)
		{
			HGPersistentHandle handle = aliases.findFirst(alias);
			if (handle == null)
				aliases.addEntry(alias, hg.getPersistentHandle(typeHandle));
			else
				throw new HGException("Alias '" + alias + "' already defined.");
		}
	}

	/**
	 * <p>
	 * Retrieve all the aliases of a given type.
	 * </p>
	 *
	 * @param typeHandle The handle of the type whose aliases are desired.
	 * @return A regular <code>HGSearchResult</code> containing the aliases. Make
	 * sure to close the result set as all other result sets in HyperGraphDB.
	 */
	public HGSearchResult<String> findAliases(HGHandle typeHandle)
	{
		return getAliases().findByValue(hg.getPersistentHandle(typeHandle));
	}

	/**
	 * <p>
	 * Remove a type alias. If the alias hasn't been previously
	 * defined, nothing is done.
	 * </p>
	 *
	 * @param alias The alias to remove. Cannot by <code>null</code>.
	 * @throws NullPointerException if <code>alias</code> is null.
	 */
	public void removeAlias(final String alias)
	{
		if (hg.getTransactionManager().getContext().getCurrent() != null)
			removeAliasTransaction(alias);
		else
			hg.getTransactionManager().transact(new Callable<Object>()
				{ public Object call() { removeAliasTransaction(alias); return null; } });
	}
	private void removeAliasTransaction(String alias)
	{
		HGBidirectionalIndex<String, HGPersistentHandle> aliases = getAliases();
		synchronized (aliases)
		{
			HGPersistentHandle handle = aliases.findFirst(alias);
			if (handle != null)
				aliases.removeEntry(alias, handle);
		}
	}

	/**
	 * <p>Permanently delete the type referred by the passed in persistent handle.</p>
	 *
	 * <p>Should be called only by HyperGraph when removing an atom that is also a type.</p>
	 *
	 * @param typeHandle
	 */
	void remove(final HGPersistentHandle typeHandle, final HGAtomType type)
	{
		if (hg.getTransactionManager().getContext().getCurrent() != null)
			removeTransaction(typeHandle, type);
		else
			hg.getTransactionManager().transact(new Callable<Object>()
				{ public Object call() { removeTransaction(typeHandle, type); return null; } });
	}
	private void removeTransaction(HGPersistentHandle typeHandle, HGAtomType type)
	{
		//
		// Remove all aliases
		//
		HGBidirectionalIndex<String, HGPersistentHandle> aliases = getAliases();
		for (Iterator<String> i = aliases.findByValue(typeHandle); i.hasNext(); )
		{
			// TODO: maybe a problem here if we are removing while iterating...
			aliases.removeEntry((String)i.next(), typeHandle);
		}

		//
		// Remove from HG type <-> Java class mappings
		//
		HGSearchResult<String> rs = null;
		try
		{
			HGBidirectionalIndex<String, HGPersistentHandle> idx = getClassToTypeDB();
			rs = idx.findByValue(typeHandle);
			while (rs.hasNext())
			{
				String classname = rs.next();
				idx.removeEntry(classname, typeHandle);
				try
				{
					// Remove from class->atom cache if there.
					Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(classname);
					classToAtomType.remove(clazz);
				}
				catch (ClassNotFoundException ex) { }
			}
		}
		catch (Throwable t)
		{
			throw new HGException(t);
		}
		finally
		{
			if (rs != null) try { rs.close(); } catch (Throwable _) { }
		}
	}

	private class ClassToTypeCache extends LinkedHashMap<Class<?>, HGHandle>
	{
		static final long serialVersionUID = -1;

		public ClassToTypeCache()
		{
			super(1000, 0.75f, true);
		}
		protected boolean removeEldestEntry(Map.Entry<Class<?>, HGHandle> eldest)
		{
			if (size() > MAX_CLASS_TO_TYPE)
			{
				if (eldest.getValue() instanceof HGLiveHandle)
				{
					HGLiveHandle h = (HGLiveHandle)eldest.getValue();
					if (h.getRef() == null)
						return true; //if it has been evicted from the atom cache, removed it from here too
					else if (hg.cache.isFrozen(h))
						return get(eldest.getKey()) == null; // this will return false and put the element on top of the list
					else
						return false; // simply return false, but don't remove since it's still in the cache
				}
				else
				{
					HGLiveHandle h = hg.cache.get((HGPersistentHandle)eldest.getValue());
					if (h != null)
					{
						eldest.setValue(h);
						return false;
					}
					else
						return true;
				}
			}
			else
				return false;
		}
	}
}
