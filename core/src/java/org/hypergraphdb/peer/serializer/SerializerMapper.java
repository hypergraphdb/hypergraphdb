package org.hypergraphdb.peer.serializer;


public interface SerializerMapper
{
	HGSerializer accept(Class<?> clazz);
	HGSerializer getSerializer();
}
