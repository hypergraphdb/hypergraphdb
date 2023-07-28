/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.mdbx;

import java.util.Arrays;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.mdbx.type.TupleBinding;
import org.hypergraphdb.storage.mdbx.type.TupleInput;
import org.hypergraphdb.storage.mdbx.type.TupleOutput;

/**
 * <p>
 * A <code>LinkBinding</code> converts a <code>HGPersistentHandle[]</code> to
 * and from a flat <code>byte[]</code> for the purposes of storage and retrieval
 * in the DB.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class LinkBinding extends TupleBinding<HGPersistentHandle[]>
{
	private MdbxStorageImplementation store;
	private HGHandleFactory handleFactory;
	private int handleSize;

	public LinkBinding(MdbxStorageImplementation store, HGHandleFactory handleFactory)
	{
		this.store = store;
		this.handleFactory = handleFactory;
		handleSize = handleFactory.nullHandle().toByteArray().length;
	}

	private HGPersistentHandle[] readHandles(byte[] buffer, int offset,
			int length)
	{
		if (length == 0)
			return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;
		if (handleSize > 0)
		{
			int handle_count = length / handleSize;
			HGPersistentHandle[] handles = new HGPersistentHandle[handle_count];
			for (int i = 0; i < handle_count; i++)
				handles[i] = handleFactory.makeHandle(buffer,
						offset + i * handleSize);
			return handles;
		}
		else
		{
			int handle_count = 0;
			HGPersistentHandle[] handles = new HGPersistentHandle[length]; // length
																			// is
																			// really
																			// max
																			// possible
			HGDataInput in = store.newDataInput(buffer, offset, length);

			while (in.available() > 0)
			{
				handles[handle_count++] = handleFactory.makeHandle(in.getBufferBytes());
			}
			return Arrays.copyOf(handles, handle_count);
		}
	}

	@Override
	public HGPersistentHandle[] entryToObject(TupleInput input)
	{
		int size = input.getBufferLength() - input.getBufferOffset();
		if (handleSize > 0 && size % handleSize != 0)
			throw new HGException(
					"While reading link tuple: the value buffer size is not a multiple of the handle size.");
		else
			return readHandles(input.getBufferBytes(), 0, size);
	}

	@Override
	public void objectToEntry(HGPersistentHandle[] link, TupleOutput output)
	{
		byte[] buffer = new byte[link.length * Math.abs(handleSize)];

		if (handleSize > 0)
		{
			for (int i = 0; i < link.length; i++)
			{
				HGPersistentHandle handle = link[i];
				System.arraycopy(handle.toByteArray(), 0, buffer,
						i * handleSize, handleSize);
			}
			output.writeFast(buffer);
		}
		else
		{
			HGDataOutput out = store.newDataOutput(buffer);
			for (int i = 0; i < link.length; i++)
			{
				HGPersistentHandle handle = link[i];
				out.write(handle.toByteArray());
			}
			output.writeFast(out.toByteArray());
		}
	}
}