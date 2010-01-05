/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
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
