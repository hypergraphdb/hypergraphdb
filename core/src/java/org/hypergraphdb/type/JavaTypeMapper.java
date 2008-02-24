package org.hypergraphdb.type;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGTypeSystem;

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
	 * the <code>javaClass</code> class.
	 * </p>
	 * 
	 * @param javaClass
	 * @param typeHandle
	 * @return
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
	 * @param typeHandle
	 * @param hgType
	 * @param javaClass
	 * @return
	 */
	HGAtomType getJavaBinding(HGHandle typeHandle, 
							  HGAtomType hgType, 
							  Class<?> javaClass); 
}