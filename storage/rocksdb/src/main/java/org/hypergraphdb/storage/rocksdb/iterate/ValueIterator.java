/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.iterate;

import org.rocksdb.AbstractNativeReference;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * An iterator which iterates over the values represented by the records contained
 * in a given RocksDB iterator
 *
 * @param <T> the type of the values to be extracted from the iterator
 */
public class ValueIterator<T>
{
	protected final BiFunction<byte[], byte[], T> recordToValue;
	protected final RocksIterator it;
	protected final Function<T, byte[]> valueToKey;
	protected final List<AbstractNativeReference> toClose;

	/**
	 *
	 * @param it the RocksIterator whose records will be converted into values
	 * @param recordToValue convertor from a raw RocksDB record to a value
	 * @param valueToKey convertor from a value to a rocksdb record. If the
	 *                   values in the Value iterator lose information i.e. cannot
	 *                   be converted back to a RocksIterator, the function must
	 *                   throw an exception
	 * @param toClose a list of native references which are assumed owned
	 *                by this ValueIterator and will be closed when the
	 *                ValueIterator itself is closed
	 */
	public ValueIterator(RocksIterator it,
			BiFunction<byte[], byte[], T> recordToValue,
			Function<T, byte[]> valueToKey,
			List<AbstractNativeReference> toClose)
	{
		this.it = it;
		this.recordToValue = recordToValue;
		this.valueToKey = valueToKey;
		this.toClose = toClose;
	}

	/**
	 * Map a ValueIterator to another ValueIterator i.e. to a ValueIterator
	 * whose values are constructed from the values of the original ValueItreator
	 * by applying a map function
	 * @param originalValueToNewValue convertor from a value in the original iterator
	 *                                to a value in the new iterator
	 * @param newValueToOriginalValue convertor from a value in the new iterator
	 *                                to a value in the original iterator.
	 *                                If the values in the new iterator lose information
	 *                                i.e. cannot be converted back to values in the
	 *                                original iterator, this function must throw an exception
	 * @return
	 * @param <U> The type of the values in the new ValueIterator
	 */
	public <U> ValueIterator<U> map(Function<T,U> originalValueToNewValue, Function<U, T> newValueToOriginalValue)
	{
		return new ValueIterator<>(
				it,
				(key, value) -> {
					var first = this.recordToValue.apply(key, value);
					var second = originalValueToNewValue.apply(first);
					return second;
				},
				value -> {
					var first = newValueToOriginalValue.apply(value);
					var second = valueToKey.apply(first);
					return second;
				},
				this.toClose);
	}

	public T current()
	{
		return recordToValue.apply(it.key(), it.value());
	}


	public void seek(T value)
	{
		it.seek(valueToKey.apply(value));
	}

	public void prev()
	{
		it.prev();
	}

	public void next()
	{
		it.next();
	}

	public void seekToFirst()
	{
		it.seekToFirst();
	}

	public boolean isValid()
	{
		return it.isValid();
	}

	public void status() throws RocksDBException
	{
		it.status();
	}

	public void seekToLast()
	{
		it.seekToLast();
	}

	public void close() throws Exception
	{
		it.close();
		for (var dep : toClose)
		{
			dep.close();
		}
	}
}

