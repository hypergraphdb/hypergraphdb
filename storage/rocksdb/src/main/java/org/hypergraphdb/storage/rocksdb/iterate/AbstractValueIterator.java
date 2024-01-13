/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.iterate;


/**
 * Interface which serves as an adapter to a Rocks
 * IteratorResultSet is backed by an iterator
 * Initially that was a normal RocksIterator.
 * 1. We needed the possibility to create a unique iterator.
 * Unique iterator does not really make sense because
 * what should be unique? key?, value? key+value?
 * 2. We want to have an abstraction for
 *
 *
 *
 */

import org.rocksdb.RocksDBException;

/**
 * More or less the same as a RocksIterator, however with a
 * defined way to extract a 'value' from a record ('value' could be the key, the value
 * or something else derived from the )
 */
public interface AbstractValueIterator<T>
		extends  AutoCloseable
{
	T current();
	void seek(T value);
	void prev();
	void next();

	void seekToFirst();

	boolean isValid();
	void status() throws RocksDBException;

	void seekToLast();

}
