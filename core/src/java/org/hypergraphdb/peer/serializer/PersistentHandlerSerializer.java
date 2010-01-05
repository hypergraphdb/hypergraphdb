/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;

public class PersistentHandlerSerializer implements HGSerializer
{
	public Object readData(InputStream in)
	{
		//TODO - get size from interface, do not hard code UUIDPersistentHandle
		return deserializePersistentHandle(in);
	}

	public void writeData(OutputStream out, Object data)
	{
		SerializationUtils.serializeInt(out, DefaultSerializerManager.PERSISTENT_HANDLE_SERIALIZER_ID);
		
		serializePersistentHandle(out, data);
	}

	public static void serializePersistentHandle(OutputStream out, Object data)
	{
		try
		{
			out.write(((HGPersistentHandle)data).toByteArray());
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	public static HGPersistentHandle deserializePersistentHandle(InputStream in)
	{
		byte[] data = new byte[UUIDPersistentHandle.SIZE];
		
		try
		{
		    for (int i = 0; i < data.length; )
		        i += in.read(data, i, data.length - i);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return UUIDPersistentHandle.makeHandle(data);
	}

}
