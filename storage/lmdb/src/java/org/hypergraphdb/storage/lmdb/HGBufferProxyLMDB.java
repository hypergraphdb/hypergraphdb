/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGPersistentHandle;

/**
 * Handle mappings between handles, raw data(byte []) and the LMDB element 
 * data type (which is either a byte[], a ByteBuffer or a DirectByteBuffer).
 */
public interface HGBufferProxyLMDB<BufferType>
{
	BufferType fromHandle(HGPersistentHandle handle);
	HGPersistentHandle toHandle(BufferType buffer);
	BufferType fromHandleArray(HGPersistentHandle [] handles);
	HGPersistentHandle [] toHandleArray(BufferType buffer);	
	BufferType fromBytes(byte [] bytes);
	byte [] toBytes(BufferType buffer);	
}
