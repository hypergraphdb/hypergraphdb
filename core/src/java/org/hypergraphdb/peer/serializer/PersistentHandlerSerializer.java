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
