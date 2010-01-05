/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * Acts as an atom type for Java interfaces and abstract classes with no
 * declared/visible bean properties. Abstract class/interfaces with declared
 * bean properties are represented by the <code>JavaAbstractBeanBinding</code>
 * class.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class JavaInterfaceBinding extends HGAbstractType 
{
	private Class<?> javaClass;
	private HGHandle typeHandle;
	private HGAtomType hgType;
	
	public JavaInterfaceBinding(HGHandle typeHandle, HGAtomType hgType, Class<?> javaClass)
	{
		this.javaClass = javaClass;
		this.typeHandle = typeHandle;
		this.hgType = hgType;
	}
	
	public Class<?> getJavaClass()
	{
		return javaClass;
	}
	
	public HGAtomType getHGType()
	{
		return hgType;
	}
	
	public HGHandle getTypeHandle()
	{
		return typeHandle;
	}
	
	public String toString()
	{
		return javaClass.toString();
	}
}
