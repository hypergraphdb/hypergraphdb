/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

public class BAtoString implements ByteArrayConverter<String> 
{
	private static final BAtoString the_instance = new BAtoString();
	
	public static BAtoString getInstance() { return the_instance; }
	
	public byte[] toByteArray(String object) 
	{
		return object.getBytes();
	}

	public String fromByteArray(byte[] byteArray, int offset, int length) 
	{
		return new String(byteArray, offset, length);
	}
}
