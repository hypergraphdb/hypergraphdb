/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted software. For permitted
 * uses, licensing options and redistribution, please see the LicensingInformation file at the root level of
 * the distribution.
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc. All rights reserved.
 */
package org.hypergraphdb.storage.bje;


import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * <p>
 * A <code>LinkBinding</code> converts a <code>UUIDPersistentHandle[]</code> to and from a flat
 * <code>byte[]</code> for the purposes of storage and retrieval in the BerkeleyDB.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class LinkBinding extends TupleBinding<HGPersistentHandle[]> {
	private HGHandleFactory handleFactory;
	private int handleSize;

	public LinkBinding(HGHandleFactory handleFactory) {
		this.handleFactory = handleFactory;
		handleSize = handleFactory.nullHandle().toByteArray().length;
	}

	public HGPersistentHandle[] readHandles(byte[] buffer, int offset, int length) {
		if (length == 0) {
			return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;
		}
		
		int handle_count = length / handleSize;
		HGPersistentHandle[] handles = new HGPersistentHandle[handle_count];
		
		for (int i = 0; i < handle_count; i++) {
			handles[i] = handleFactory.makeHandle(buffer, offset + i * handleSize);
		}
		
		return handles;
	}

	public HGPersistentHandle[] entryToObject(TupleInput input) {
		int size = input.getBufferLength() - input.getBufferOffset();
		if (size % handleSize != 0) {
			throw new HGException(
					"While reading link tuple: the value buffer size is not a multiple of the handle size.");
		}
		else {
			return readHandles(input.getBufferBytes(), 0, size);
		}
	}

	public void objectToEntry(HGPersistentHandle[] link, TupleOutput output) {
		//HGPersistentHandle[] link = (HGPersistentHandle[])x;
		byte[] buffer = new byte[link.length * handleSize];
		for (int i = 0; i < link.length; i++) {
			HGPersistentHandle handle = (HGPersistentHandle)link[i];
			System.arraycopy(handle.toByteArray(), 0, buffer, i * handleSize, handleSize);
		}
		output.writeFast(buffer);
	}
}