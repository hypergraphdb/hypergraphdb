package org.hypergraphdb.storage.lmdb.type.util.unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import org.hypergraphdb.storage.lmdb.type.util.IsAndroid;

public class Unsafe
{
	/** unsafe may be null on android devices */
	public static sun.misc.Unsafe UNSAFE;
	public static int ADDRESS_SIZE;
	public static long ARRAY_BASE_OFFSET;

	static
	{
		if (!IsAndroid.isAndroid)
		{
			try
			{
				final PrivilegedExceptionAction<sun.misc.Unsafe> action = new PrivilegedExceptionAction<sun.misc.Unsafe>()
				{
					public sun.misc.Unsafe run() throws Exception
					{
						final Field field = sun.misc.Unsafe.class
								.getDeclaredField("theUnsafe");
						field.setAccessible(true);
						return (sun.misc.Unsafe) field.get(null);
					}
				};

				UNSAFE = AccessController.doPrivileged(action);
				ADDRESS_SIZE = UNSAFE.addressSize();
				ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
			}
			catch (final Exception ex)
			{
				throw new RuntimeException(ex);
			}
		}
	}

	public static long getAddress(long address, int offset)
	{
		return Unsafe.UNSAFE.getAddress(address + Unsafe.ADDRESS_SIZE * offset);
	}

	public static long getLong(long address, int offset)
	{
		return Unsafe.UNSAFE.getLong(address + Unsafe.ADDRESS_SIZE * offset);
	}

	public static void putLong(long address, int offset, long value)
	{
		UNSAFE.putLong(null, address + Unsafe.ADDRESS_SIZE * offset, value);
	}

	public static void getBytes(long address, int index, byte[] key)
	{
		UNSAFE.copyMemory(null, address + index, key, ARRAY_BASE_OFFSET,
				key.length);
	}
}
