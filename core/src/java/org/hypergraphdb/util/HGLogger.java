/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

public class HGLogger 
{
	public void trace(String msg)
	{
		System.out.println(msg);
	}
	
	public void warning(String msg)
	{
		System.err.println("[HGWARN] - " + msg);
	}

	public void warning(String msg, Throwable t)
	{
		System.err.println("[HGWARN] - " + msg);
		t.printStackTrace(System.out);
	}
	
	public void exception(Throwable t)
	{
		System.err.println("[HGEXCEPTION] - " + t.toString());
		t.printStackTrace(System.out);		
	}
}
