package org.hypergraphdb.storage.lmdb.type.util;

import java.util.Comparator;

import org.hypergraphdb.storage.lmdb.type.util.unsafe.Unsafe;

public final class ByteArrayComparator
{
	Comparator<byte[]> comparator;

	public ByteArrayComparator(Comparator<byte[]> comparator)
	{
		this.comparator = comparator;
	}

	public long compare(long ptr1, long ptr2)
	{
		int size = (int) Unsafe.getLong(ptr1, 0);
		long address = Unsafe.getAddress(ptr1, 1);
		DirectBuffer key1 = new DirectBuffer();
		key1.wrap(address, size);
		byte[] key1Bytes = new byte[size];
		key1.getBytes(0, key1Bytes);
		size = (int) Unsafe.getLong(ptr2, 0);
		address = Unsafe.getAddress(ptr2, 1);
		DirectBuffer key2 = new DirectBuffer();
		key2.wrap(address, size);
		byte[] key2Bytes = new byte[size];
		key2.getBytes(0, key2Bytes);
		return comparator.compare(key1Bytes, key2Bytes);
	}
}
