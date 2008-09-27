package org.hypergraphdb.peer.protocol;

import java.io.InputStream;

import org.hypergraphdb.peer.serializer.HGSerializer;

/**
 * @author Cipri Costa
 * Retrieves serializers based on different criteria.
 */
public interface SerializerManager
{
	public HGSerializer getSerializer(InputStream in);
	public HGSerializer getSerializer(Object data);
	public HGSerializer getSerializerByType(Class<?> clazz);


}
