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

public class ValueIterator<T> implements AbstractValueIterator<T>
{
	protected final BiFunction<byte[], byte[], T> recordToValue;
	protected final RocksIterator it;
	protected final Function<T, byte[]> valueToKey;
	protected final List<AbstractNativeReference> toClose;

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

	/*

	 */

	/**
	 * Map t
	 * @param originalValueToNewValue
	 * @param newValueToOriginalValue
	 * @return
	 * @param <U>
	 */
	public <U> ValueIterator<U> map(Function<T,U> originalValueToNewValue, Function<U, T> newValueToOriginalValue)
	{
//		return new ValueIterator<>(it, this.recordToValue.andThen(originalValueToNewValue), newValueToOriginalValue.andThen(this.valueToKey), this.toClose);
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
	@Override
	public T current()
	{
		return recordToValue.apply(it.key(), it.value());
	}

	@Override
	public void seek(T value)
	{
		it.seek(valueToKey.apply(value));
	}

	@Override
	public void prev()
	{
		it.prev();
	}

	@Override
	public void next()
	{
		it.next();
	}

	@Override
	public void seekToFirst()
	{
		it.seekToFirst();
	}

	@Override
	public boolean isValid()
	{
		return it.isValid();
	}

	@Override
	public void status() throws RocksDBException
	{
		it.status();
	}

	@Override
	public void seekToLast()
	{
		it.seekToLast();
	}

	@Override
	public void close() throws Exception
	{
		it.close();
		for (var dep : toClose)
		{
			dep.close();
		}
	}
}

