package org.hypergraphdb.peer.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import org.hypergraphdb.peer.serializer.CustomSerializedValue;
//import org.hypergraphdb.peer.serializer.DefaultSerializerManager;
import org.hypergraphdb.peer.serializer.GenericSerializer;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.serializer.JSONWriter;
import org.hypergraphdb.peer.serializer.SerializationUtils;



/**
 * @author Cipri Costa
 *
 * <p>
 * The root of all the serialization/deserialization mechanism.
 * </p>
 */
public class ObjectSerializer
{
	private static final byte[] DATA_SIGNATURE = "DATA".getBytes();
	private static final byte[] END_SIGNATURE = "END".getBytes();
	
//	private static SerializerManager serializationManager = new DefaultSerializerManager();
	
	public ObjectSerializer()
	{
	}

	public void serialize(OutputStream out, Object data) throws IOException
	{
		ProtocolUtils.writeSignature(out, DATA_SIGNATURE);
		JSONWriter writer = new JSONWriter();
		SerializationUtils.serializeString(out, writer.write(data));
		
		ArrayList<CustomSerializedValue> customValues = writer.getCustomValues();
		SerializationUtils.serializeInt(out, customValues.size());
		if(customValues.size() > 0)
		{
			GenericSerializer serializer = new GenericSerializer();
			for(CustomSerializedValue value : customValues)
			{
				serializer.writeData(out, value.get());
			}
		}

		//serializer.writeData(out, data);
		ProtocolUtils.writeSignature(out, END_SIGNATURE);
	}
	
	public Object deserialize(InputStream in) throws IOException 
	{
		Object result = null;
		
		if (ProtocolUtils.verifySignature(in, DATA_SIGNATURE))
		{
			//result = serializationManager.getSerializer(in).readData(in);
			JSONReader reader = new JSONReader();
			result = reader.read(SerializationUtils.deserializeString(in));
						
			int size = SerializationUtils.deserializeInt(in);
			if (size > 0)
			{
				HashMap<Integer, CustomSerializedValue> customValues = reader.getCustomValues();
				GenericSerializer serializer = new GenericSerializer();

				for(Integer i=0;i<size;i++)
				{
					Object value = serializer.readData(in);
					if (customValues.containsKey(i))
						customValues.get(i).setValue(value);
				}
			}
			
			if (!ProtocolUtils.verifySignature(in, END_SIGNATURE))
			{
				result = null;
			}
		}

		return result;
	}
	
}
