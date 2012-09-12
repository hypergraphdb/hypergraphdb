/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

public class HGEMissingData extends HGException
{
	private static final long serialVersionUID = -1;
	private HGPersistentHandle handle;

	public HGEMissingData()
	{
		super("Missing data in storage.");
	}
	
	public HGEMissingData(HGPersistentHandle handle)
	{
		super("Missing data in storage for handle '" + handle + "'");
		this.handle = handle;
	}
	
	public HGPersistentHandle getHandle()
	{
		return handle;
	}

	public void setHandle(HGPersistentHandle handle)
	{
		this.handle = handle;
	}	
}
