/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * A <code>LinkBinding</code> converts a <code>UUIDPersistentHandle[]</code>
 * to and from a flat <code>byte[]</code> for the purposes of storage and
 * retrieval in the BerkeleyDB.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class LinkBinding extends TupleBinding
{
	public static HGPersistentHandle [] readHandles(byte [] buffer, int offset, int length)
	{
        if (length == 0)
            return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;
        int handle_count = length / UUIDPersistentHandle.SIZE;
        HGPersistentHandle [] handles = new HGPersistentHandle[handle_count];
        for (int i = 0; i < handle_count; i++)
            handles[i] = UUIDPersistentHandle.makeHandle(buffer, offset + i*UUIDPersistentHandle.SIZE);
        return handles;
	}
	
    public Object entryToObject(TupleInput input)
    {
        int size = input.getBufferLength() - input.getBufferOffset();
        if (size % UUIDPersistentHandle.SIZE != 0)
            throw new HGException("While reading link tuple: the value buffer size is not a multiple of the handle size.");
        else
        	return readHandles(input.getBufferBytes(), 0, size);
    }

    public void objectToEntry(Object object, TupleOutput output)
    {
        if ( !(object instanceof UUIDPersistentHandle []))
           if(!(object instanceof HGPersistentHandle []))
              throw new HGException("Attempt to store an object which is not a handle array as a link tuple.");
        //UUIDPersistentHandle [] link = (UUIDPersistentHandle [])object;
        HGPersistentHandle [] link = (HGPersistentHandle [])object;
        byte [] buffer = new byte[link.length * UUIDPersistentHandle.SIZE];
        for (int i = 0; i < link.length; i++)
        {
            UUIDPersistentHandle handle = (UUIDPersistentHandle)link[i];
            System.arraycopy(handle.toByteArray(), 0, 
                             buffer, i*UUIDPersistentHandle.SIZE, 
                             UUIDPersistentHandle.SIZE);            
        }
        output.writeFast(buffer);
    }
}