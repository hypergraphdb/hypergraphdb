/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;

public class HGByteArrayBufferProxyLMDB implements HGBufferProxyLMDB<byte[]>
{
	private HGHandleFactory factory;
	private int handleSize;
	
	public HGByteArrayBufferProxyLMDB(HGHandleFactory factory)
	{
		this.factory = factory;
		this.handleSize = factory.nullHandle().toByteArray().length;
	}
	
	@Override
	public byte [] fromHandle(HGPersistentHandle handle)
	{
		return handle.toByteArray();
	}

	@Override
	public HGPersistentHandle toHandle(byte [] buffer)
	{
		return this.factory.makeHandle(buffer);
	}

	
	@Override
	public byte[] fromHandleArray(HGPersistentHandle[] handles)
	{
		byte [] result = new byte[handles.length * handleSize];
		for (int i = 0; i < handles.length; i++)
			System.arraycopy(handles[i].toByteArray(), 0, result, i*handleSize, handleSize);
		return result;
	}

	@Override
	public HGPersistentHandle[] toHandleArray(byte[] buffer)
	{
	    if (buffer == null)
	        return null;
		HGPersistentHandle [] handles= new HGPersistentHandle[buffer.length / handleSize];
		for (int i = 0; i < handles.length; i++)
			handles[i] = this.factory.makeHandle(buffer, i*handleSize);
		return handles;
	}

	@Override
	public byte[] fromBytes(byte[] bytes)
	{
		return bytes;
	}

	@Override
	public byte[] toBytes(byte[] buffer)
	{
		return buffer;
	}

	
}
