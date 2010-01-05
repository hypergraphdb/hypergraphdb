/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGPersistentHandle;

/**
 * <p>
 * This is a simple structure that hold information about a predefined HyperGraph
 * type. Predefined types are generally recorded in a HyperGraph when it is created.
 * However, in a distributed setting, applications may need to share some domain/implementation
 * specific data and therefore predefined types may be plugged into a HyperGraph system
 * at a later time. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class PredefinedTypeDescriptor
{
	private HGPersistentHandle handle = null;
	private String implementationClassName = null;
	private String [] supportedClasses = null;
	
	public PredefinedTypeDescriptor()
	{		
	}

	public PredefinedTypeDescriptor(HGPersistentHandle handle, 
									String implementationClassName)
	{
		this.handle = handle;
		this.implementationClassName = implementationClassName;
	}

	public PredefinedTypeDescriptor(HGPersistentHandle handle, 
								    String implementationClassName, 
								    String [] supportedClasses)
	{
		this.handle = handle;
		this.implementationClassName = implementationClassName;
		this.supportedClasses = supportedClasses;
	}
	
	public HGPersistentHandle getHandle()
	{
		return handle;
	}

	public void setHandle(HGPersistentHandle handle)
	{
		this.handle = handle;
	}

	public String getImplementationClassName()
	{
		return implementationClassName;
	}

	public void setImplementationClassName(String implementationClassName)
	{
		this.implementationClassName = implementationClassName;
	}

	public String[] getSupportedClasses()
	{
		return supportedClasses;
	}

	public void setSupportedClasses(String[] supportedClasses)
	{
		this.supportedClasses = supportedClasses;
	}	
}
