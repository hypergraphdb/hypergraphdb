/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.serializer;

import java.io.InputStream;
import java.io.OutputStream;


public class NullSerializer implements HGSerializer {

	public Object readData(InputStream in)
	{
		return null;
	}

	public void writeData(OutputStream out, Object data)
	{
		SerializationUtils.serializeInt(out, DefaultSerializerManager.NULL_SERIALIZER_ID);		
	}

}
