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
import org.rocksdb.RocksIterator;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DistinctValueIterator<T> extends ValueIterator<T>
{
	public DistinctValueIterator(RocksIterator it,
			BiFunction<byte[], byte[], T> toValue,
			Function<T, byte[]> valueToKey,
			List<AbstractNativeReference> toClose)
	{
		super(it, toValue, valueToKey, toClose);
	}

	@Override
	public <U> DistinctValueIterator<U> map(Function<T,U> originalValueToNewValue, Function<U, T> newValueToOriginalValue)
	{
		return new DistinctValueIterator<>(it,
				this.recordToValue.andThen(originalValueToNewValue),
				newValueToOriginalValue.andThen(this.valueToKey),
				this.toClose);
	}

	@Override
	public void prev()
	{
		var current = this.current();
		while (true)
		{
			it.prev();
			if (it.isValid())
			{
				var prev = this.current();
				if (!prev.equals(current))
				{
					break;
				}
			}
			else
			{
				break;
			}
		}
	}

	@Override
	public void next()
	{
		var current = this.current();
		while (true)
		{
			it.next();
			if (it.isValid())
			{
				var prev = this.current();
				if (!prev.equals(current))
				{
					break;
				}
			}
			else
			{
				break;
			}
		}
	}
}
