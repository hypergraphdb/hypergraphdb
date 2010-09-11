/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGHandle;

/**
 * 
 * <p>
 * A <code>JavaTypeMapper</code> is used to create HyperGraphDB type out of Java classes and
 * to provide appropriate run-time representations. 
 * </p>
 *
 * <p>
 * When the type an atom of an unknown Java class is first requested, the type system must
 * create an appropriate HyperGraphDB type using Java reflection on the class. The HyperGraphDB
 * type should be portable and potentially usable from other languages such as C++. For example
 * a plain Java bean is usually translated into the general <code>RecordType</code>.    
 * </p>
 * 
 * <p>
 * The HyperGraphDB type, being general, doesn't have to necessarily create objects of the
 * original Java class. In other words 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface JavaTypeMapper extends HGGraphHolder
{	
	/**
	 * <p>
	 * Create a new HyperGraphDB type for the given Java class. The <code>HGHandle</code>
	 * of the type pre-created and provided as a parameter.  
	 * </p>
	 * 
	 * <p>
	 * This method should return a brand new <code>HGAtomType</code> that will subsequently
	 * be saved as a type atom with handle <code>typeHandle</code> and associated with
	 * the <code>javaClass</code> class. The method should return <code>null</code> in
	 * case it cannot (or it determines that it should not) create a <code>HGAtomType</code>.
	 * In the case the JavaTypeFactory will move on to try the next of the registered
	 * type mappers.
	 * </p>
	 * 
	 * @param javaClass
	 * @param typeHandle
	 * @return A newly created <code>HGAtomType</code> corresponding to the passed in Java
	 * class or <code>null</code> if such a type could not be created.
	 */
	HGAtomType defineHGType(Class<?> javaClass, 
							HGHandle typeHandle);
	
	/**
	 * <p>
	 * Create a type wrapper for a given raw HyperGraphDB type. The type wrapper should
	 * work with the regular Java runtime instance of an atom and use the underlying 
	 * HG type for actual storage and retrieval. 
	 * </p>
	 * 
	 * @param typeHandle The handle of the type being wrapped.
	 * @param hgType The HyperGraphDB type instance to be wrapped
	 * @param javaClass The Java class corresponding to this HyperGraphDB type.
	 * @return The type wrapper or <code>null</code> if 
	 */
	HGAtomType getJavaBinding(HGHandle typeHandle, 
							  HGAtomType hgType, 
							  Class<?> javaClass); 
}
