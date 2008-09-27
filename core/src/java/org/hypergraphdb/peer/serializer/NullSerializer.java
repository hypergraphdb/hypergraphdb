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
