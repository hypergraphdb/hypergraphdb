package org.hypergraphdb.storage.lmdb.type.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

/**
 * Supports regular, byte ordered, and atomic (memory ordered) access to an
 * underlying buffer. The buffer can be a byte[] or one of the various
 * {@link java.nio.ByteBuffer} implementations.
 */
public class DirectBuffer
{
	/** Size of a byte in bytes */
	public static final int SIZE_OF_BYTE = 1;

	/** Size of a boolean in bytes */
	public static final int SIZE_OF_BOOLEAN = 1;

	/** Size of a char in bytes */
	public static final int SIZE_OF_CHAR = 2;

	/** Size of a short in bytes */
	public static final int SIZE_OF_SHORT = 2;

	/** Size of an int in bytes */
	public static final int SIZE_OF_INT = 4;

	/** Size of a a float in bytes */
	public static final int SIZE_OF_FLOAT = 4;

	/** Size of a long in bytes */
	public static final int SIZE_OF_LONG = 8;

	/** Size of a double in bytes */
	public static final int SIZE_OF_DOUBLE = 8;

	private static final Unsafe UNSAFE;

	static
	{
		try
		{
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>()
			{
				public Unsafe run() throws Exception
				{
					final Field field = Unsafe.class
							.getDeclaredField("theUnsafe");
					field.setAccessible(true);
					return (Unsafe) field.get(null);
				}
			};

			UNSAFE = AccessController.doPrivileged(action);
		}
		catch (final Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Get the instance of {@link sun.misc.Unsafe}.
	 *
	 * @return the instance of Unsafe
	 */
	public static Unsafe getUnsafe()
	{
		return UNSAFE;
	}

	public static final String DISABLE_BOUNDS_CHECKS_PROP_NAME = "directbuffer.disable.bounds.checks";
	public static final boolean SHOULD_BOUNDS_CHECK = !Boolean
			.getBoolean(DISABLE_BOUNDS_CHECKS_PROP_NAME);

	private static final byte[] NULL_BYTES = "null"
			.getBytes(StandardCharsets.UTF_8);
	private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
	private static final long ARRAY_BASE_OFFSET = UNSAFE
			.arrayBaseOffset(byte[].class);

	private byte[] byteArray;
	private ByteBuffer byteBuffer;
	private long addressOffset;

	private int capacity;

	/**
	 * Attach a view to a direct {@link java.nio.ByteBuffer}.
	 */
	public DirectBuffer()
	{
		wrap(ByteBuffer.allocateDirect(511));
	}

	/**
	 * Attach a view to a byte[] for providing direct access.
	 *
	 * @param buffer
	 *            to which the view is attached.
	 */
	public DirectBuffer(final byte[] buffer)
	{
		wrap(buffer);
	}

	/**
	 * Attach a view to a {@link java.nio.ByteBuffer} for providing direct
	 * access, the {@link java.nio.ByteBuffer} can be heap based or direct.
	 *
	 * @param buffer
	 *            to which the view is attached.
	 */
	public DirectBuffer(final ByteBuffer buffer)
	{
		wrap(buffer);
	}

	/**
	 * Attach a view to an off-heap memory region by address.
	 *
	 * @param address
	 *            where the memory begins off-heap
	 * @param capacity
	 *            of the buffer from the given address
	 */
	public DirectBuffer(final long address, final int capacity)
	{
		wrap(address, capacity);
	}

	/**
	 * Attach a view to an existing {@link DirectBuffer}
	 *
	 * @param buffer
	 *            to which the view is attached.
	 */
	public DirectBuffer(final DirectBuffer buffer)
	{
		wrap(buffer);
	}

	public void wrap(final byte[] buffer)
	{
		addressOffset = ARRAY_BASE_OFFSET;
		capacity = buffer.length;
		byteArray = buffer;
		byteBuffer = null;
	}

	public void wrap(final ByteBuffer buffer)
	{
		byteBuffer = buffer;

		if (buffer.hasArray())
		{
			byteArray = buffer.array();
			addressOffset = ARRAY_BASE_OFFSET + buffer.arrayOffset();
		}
		else
		{
			byteArray = null;
			addressOffset = ((sun.nio.ch.DirectBuffer) buffer).address();
		}

		capacity = buffer.capacity();
	}

	public void wrap(final long address, final int capacity)
	{
		addressOffset = address;
		this.capacity = capacity;
		byteArray = null;
		byteBuffer = null;
	}

	public void wrap(final DirectBuffer buffer)
	{
		addressOffset = buffer.addressOffset();
		capacity = buffer.capacity();
		byteArray = buffer.byteArray();
		byteBuffer = buffer.byteBuffer();
	}

	public long addressOffset()
	{
		return addressOffset;
	}

	public byte[] byteArray()
	{
		return byteArray;
	}

	public ByteBuffer byteBuffer()
	{
		return byteBuffer;
	}

	public void setMemory(final int index, final int length, final byte value)
	{
		boundsCheck(index, length);

		UNSAFE.setMemory(byteArray, addressOffset + index, length, value);
	}

	public int capacity()
	{
		return capacity;
	}

	public void checkLimit(final int limit)
	{
		if (limit > capacity)
		{
			final String msg = String.format("limit=%d is beyond capacity=%d",
					limit, capacity);
			throw new IndexOutOfBoundsException(msg);
		}
	}

	public ByteBuffer duplicateByteBuffer()
	{
		if (null == byteBuffer)
		{
			return ByteBuffer.wrap(byteArray);
		}
		else
		{
			final ByteBuffer duplicate = byteBuffer.duplicate();
			duplicate.clear();

			return duplicate;
		}
	}

	///////////////////////////////////////////////////////////////////////////

	public long getLong(final int index, final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_LONG);

		long bits = UNSAFE.getLong(byteArray, addressOffset + index);
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Long.reverseBytes(bits);
		}

		return bits;
	}

	public void putLong(final int index, final long value,
			final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_LONG);

		long bits = value;
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Long.reverseBytes(bits);
		}

		UNSAFE.putLong(byteArray, addressOffset + index, bits);
	}

	public long getLong(final int index)
	{
		boundsCheck(index, SIZE_OF_LONG);

		return UNSAFE.getLong(byteArray, addressOffset + index);
	}

	public void putLong(final int index, final long value)
	{
		boundsCheck(index, SIZE_OF_LONG);

		UNSAFE.putLong(byteArray, addressOffset + index, value);
	}

	public long getLongVolatile(final int index)
	{
		boundsCheck(index, SIZE_OF_LONG);

		return UNSAFE.getLongVolatile(byteArray, addressOffset + index);
	}

	public void putLongVolatile(final int index, final long value)
	{
		boundsCheck(index, SIZE_OF_LONG);

		UNSAFE.putLongVolatile(byteArray, addressOffset + index, value);
	}

	public void putLongOrdered(final int index, final long value)
	{
		boundsCheck(index, SIZE_OF_LONG);

		UNSAFE.putOrderedLong(byteArray, addressOffset + index, value);
	}

	public void addLongOrdered(final int index, final long increment)
	{
		boundsCheck(index, SIZE_OF_LONG);

		final long offset = addressOffset + index;
		final byte[] byteArray = this.byteArray;
		final long value = UNSAFE.getLong(byteArray, offset);
		UNSAFE.putOrderedLong(byteArray, offset, value + increment);
	}

	public boolean compareAndSetLong(final int index, final long expectedValue,
			final long updateValue)
	{
		boundsCheck(index, SIZE_OF_LONG);

		return UNSAFE.compareAndSwapLong(byteArray, addressOffset + index,
				expectedValue, updateValue);
	}

	///////////////////////////////////////////////////////////////////////////

	public int getInt(final int index, final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_INT);

		int bits = UNSAFE.getInt(byteArray, addressOffset + index);
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Integer.reverseBytes(bits);
		}

		return bits;
	}

	public void putInt(final int index, final int value,
			final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_INT);

		int bits = value;
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Integer.reverseBytes(bits);
		}

		UNSAFE.putInt(byteArray, addressOffset + index, bits);
	}

	public int getInt(final int index)
	{
		boundsCheck(index, SIZE_OF_INT);

		return UNSAFE.getInt(byteArray, addressOffset + index);
	}

	public void putInt(final int index, final int value)
	{
		boundsCheck(index, SIZE_OF_INT);

		UNSAFE.putInt(byteArray, addressOffset + index, value);
	}

	public int getIntVolatile(final int index)
	{
		boundsCheck(index, SIZE_OF_INT);

		return UNSAFE.getIntVolatile(byteArray, addressOffset + index);
	}

	public void putIntVolatile(final int index, final int value)
	{
		boundsCheck(index, SIZE_OF_INT);

		UNSAFE.putIntVolatile(byteArray, addressOffset + index, value);
	}

	public void putIntOrdered(final int index, final int value)
	{
		boundsCheck(index, SIZE_OF_INT);

		UNSAFE.putOrderedInt(byteArray, addressOffset + index, value);
	}

	public void addIntOrdered(final int index, final int increment)
	{
		boundsCheck(index, SIZE_OF_INT);

		final long offset = addressOffset + index;
		final byte[] byteArray = this.byteArray;
		final int value = UNSAFE.getInt(byteArray, offset);
		UNSAFE.putOrderedInt(byteArray, offset, value + increment);
	}

	public boolean compareAndSetInt(final int index, final int expectedValue,
			final int updateValue)
	{
		boundsCheck(index, SIZE_OF_INT);

		return UNSAFE.compareAndSwapInt(byteArray, addressOffset + index,
				expectedValue, updateValue);
	}

	///////////////////////////////////////////////////////////////////////////

	public double getDouble(final int index, final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_DOUBLE);

		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			final long bits = UNSAFE.getLong(byteArray, addressOffset + index);
			return Double.longBitsToDouble(Long.reverseBytes(bits));
		}
		else
		{
			return UNSAFE.getDouble(byteArray, addressOffset + index);
		}
	}

	public void putDouble(final int index, final double value,
			final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_DOUBLE);

		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			final long bits = Long
					.reverseBytes(Double.doubleToRawLongBits(value));
			UNSAFE.putLong(byteArray, addressOffset + index, bits);
		}
		else
		{
			UNSAFE.putDouble(byteArray, addressOffset + index, value);
		}
	}

	public double getDouble(final int index)
	{
		boundsCheck(index, SIZE_OF_DOUBLE);

		return UNSAFE.getDouble(byteArray, addressOffset + index);
	}

	public void putDouble(final int index, final double value)
	{
		boundsCheck(index, SIZE_OF_DOUBLE);

		UNSAFE.putDouble(byteArray, addressOffset + index, value);
	}

	///////////////////////////////////////////////////////////////////////////

	public float getFloat(final int index, final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_FLOAT);

		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			final int bits = UNSAFE.getInt(byteArray, addressOffset + index);
			return Float.intBitsToFloat(Integer.reverseBytes(bits));
		}
		else
		{
			return UNSAFE.getFloat(byteArray, addressOffset + index);
		}
	}

	public void putFloat(final int index, final float value,
			final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_FLOAT);

		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			final int bits = Integer
					.reverseBytes(Float.floatToRawIntBits(value));
			UNSAFE.putLong(byteArray, addressOffset + index, bits);
		}
		else
		{
			UNSAFE.putFloat(byteArray, addressOffset + index, value);
		}
	}

	public float getFloat(final int index)
	{
		boundsCheck(index, SIZE_OF_FLOAT);

		return UNSAFE.getFloat(byteArray, addressOffset + index);
	}

	public void putFloat(final int index, final float value)
	{
		boundsCheck(index, SIZE_OF_FLOAT);

		UNSAFE.putFloat(byteArray, addressOffset + index, value);
	}

	///////////////////////////////////////////////////////////////////////////

	public short getShort(final int index, final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		short bits = UNSAFE.getShort(byteArray, addressOffset + index);
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Short.reverseBytes(bits);
		}

		return bits;
	}

	public void putShort(final int index, final short value,
			final ByteOrder byteOrder)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		short bits = value;
		if (NATIVE_BYTE_ORDER != byteOrder)
		{
			bits = Short.reverseBytes(bits);
		}

		UNSAFE.putShort(byteArray, addressOffset + index, bits);
	}

	public short getShort(final int index)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		return UNSAFE.getShort(byteArray, addressOffset + index);
	}

	public void putShort(final int index, final short value)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		UNSAFE.putShort(byteArray, addressOffset + index, value);
	}

	public short getShortVolatile(final int index)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		return UNSAFE.getShortVolatile(byteArray, addressOffset + index);
	}

	public void putShortVolatile(final int index, final short value)
	{
		boundsCheck(index, SIZE_OF_SHORT);

		UNSAFE.putShortVolatile(byteArray, addressOffset + index, value);
	}

	///////////////////////////////////////////////////////////////////////////

	public byte getByte(final int index)
	{
		boundsCheck(index, SIZE_OF_BYTE);

		return UNSAFE.getByte(byteArray, addressOffset + index);
	}

	public void putByte(final int index, final byte value)
	{
		boundsCheck(index, SIZE_OF_BYTE);

		UNSAFE.putByte(byteArray, addressOffset + index, value);
	}

	public int getBytes(final int index, final byte[] dst)
	{
		return getBytes(index, dst, 0, dst.length);
	}

	public int getBytes(final int index, final byte[] dst, final int offset,
			final int length)
	{
		int count = Math.min(length, capacity - index);
		count = Math.min(count, dst.length);

		boundsCheck(index, count);

		UNSAFE.copyMemory(byteArray, addressOffset + index, dst,
				ARRAY_BASE_OFFSET + offset, count);

		return count;
	}

	public void getBytes(final int index, final DirectBuffer dstBuffer,
			final int dstIndex, final int length)
	{
		dstBuffer.putBytes(dstIndex, this, index, length);
	}

	public int getBytes(final int index, final ByteBuffer dstBuffer,
			final int length)
	{
		int count = Math.min(dstBuffer.remaining(), capacity - index);
		count = Math.min(count, length);

		boundsCheck(index, count);

		final int dstOffset = dstBuffer.position();
		final byte[] dstByteArray;
		final long dstBaseOffset;
		if (dstBuffer.hasArray())
		{
			dstByteArray = dstBuffer.array();
			dstBaseOffset = ARRAY_BASE_OFFSET + dstBuffer.arrayOffset();
		}
		else
		{
			dstByteArray = null;
			dstBaseOffset = ((sun.nio.ch.DirectBuffer) dstBuffer).address();
		}

		UNSAFE.copyMemory(byteArray, addressOffset + index, dstByteArray,
				dstBaseOffset + dstOffset, count);
		dstBuffer.position(dstBuffer.position() + count);

		return count;
	}

	public int putBytes(final int index, final byte[] src)
	{
		return putBytes(index, src, 0, src.length);
	}

	public int putBytes(final int index, final byte[] src, final int offset,
			final int length)
	{
		int count = Math.min(length, capacity - index);
		count = Math.min(count, src.length);

		boundsCheck(index, count);

		UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray,
				addressOffset + index, length);

		return length;
	}

	public int putBytes(final int index, final ByteBuffer srcBuffer,
			final int length)
	{
		int count = Math.min(srcBuffer.remaining(), length);
		count = Math.min(count, capacity - index);

		boundsCheck(index, count);

		count = putBytes(index, srcBuffer, srcBuffer.position(), count);
		srcBuffer.position(srcBuffer.position() + count);

		return count;
	}

	public int putBytes(final int index, final ByteBuffer srcBuffer,
			final int srcIndex, final int length)
	{
		int count = Math.min(length, capacity - index);
		count = Math.min(count, srcBuffer.capacity() - srcIndex);

		boundsCheck(index, count);

		final byte[] srcByteArray;
		final long srcBaseOffset;
		if (srcBuffer.hasArray())
		{
			srcByteArray = srcBuffer.array();
			srcBaseOffset = ARRAY_BASE_OFFSET + srcBuffer.arrayOffset()
					+ srcIndex;
		}
		else
		{
			srcByteArray = null;
			srcBaseOffset = ((sun.nio.ch.DirectBuffer) srcBuffer).address();
		}

		UNSAFE.copyMemory(srcByteArray, srcBaseOffset + srcIndex, byteArray,
				addressOffset + index, count);

		return count;
	}

	public void putBytes(final int index, final DirectBuffer srcBuffer,
			final int srcIndex, final int length)
	{
		boundsCheck(index, length);
		srcBuffer.boundsCheck(srcIndex, length);

		UNSAFE.copyMemory(srcBuffer.byteArray(),
				srcBuffer.addressOffset() + srcIndex, byteArray,
				addressOffset + index, length);
	}

	///////////////////////////////////////////////////////////////////////////

	public String getStringUtf8(final int offset, final ByteOrder byteOrder)
	{
		final int length = getInt(offset, byteOrder);

		return getStringUtf8(offset, length);
	}

	public String getStringUtf8(final int offset, final int length)
	{
		final byte[] stringInBytes = new byte[length];
		getBytes(offset + SIZE_OF_INT, stringInBytes);

		return new String(stringInBytes, StandardCharsets.UTF_8);
	}

	public int putStringUtf8(final int offset, final String value,
			final ByteOrder byteOrder, final int maxEncodedSize)
	{
		final byte[] bytes = value != null
				? value.getBytes(StandardCharsets.UTF_8) : NULL_BYTES;
		if (bytes.length > maxEncodedSize)
		{
			throw new IllegalArgumentException(
					"Encoded string larger than maximum size: "
							+ maxEncodedSize);
		}

		putInt(offset, bytes.length, byteOrder);
		putBytes(offset + SIZE_OF_INT, bytes);

		return SIZE_OF_INT + bytes.length;
	}

	public String getStringWithoutLengthUtf8(final int offset, final int length)
	{
		final byte[] stringInBytes = new byte[length];
		getBytes(offset, stringInBytes);

		return new String(stringInBytes, StandardCharsets.UTF_8);
	}

	public int putStringWithoutLengthUtf8(final int offset, final String value)
	{
		final byte[] bytes = value != null
				? value.getBytes(StandardCharsets.UTF_8) : NULL_BYTES;
		putBytes(offset, bytes);

		return bytes.length;
	}

	///////////////////////////////////////////////////////////////////////////

	public void boundsCheck(final int index, final int length)
	{
		if (SHOULD_BOUNDS_CHECK)
		{
			if (index < 0 || length < 0 || (index + length) > capacity)
			{
				throw new IndexOutOfBoundsException(
						String.format("index=%d, length=%d, capacity=%d", index,
								length, capacity));
			}
		}
	}

	public ByteString getString(final int offset)
	{
		byte terminator = 1;
		int index = offset;
		while (terminator != (byte) 0x00)
		{
			boundsCheck(index, SIZE_OF_BYTE);
			terminator = UNSAFE.getByte(byteArray, addressOffset + index++);
		}
		byte[] bytes = new byte[index - offset - 1];
		getBytes(offset, bytes);
		return new ByteString(bytes);
	}

	public void putString(final int offset, final ByteString value)
	{
		final byte[] bytes = value.getBytes();
		putBytes(offset, bytes);
		putByte(offset + bytes.length, (byte) 0x00);
	}

	public boolean getBoolean(int offset)
	{
		boundsCheck(offset, SIZE_OF_BYTE);
		return UNSAFE.getBoolean(byteArray, addressOffset + offset);
	}

	public void putBoolean(int index, boolean value)
	{
		boundsCheck(index, SIZE_OF_BOOLEAN);
		UNSAFE.putBoolean(byteArray, addressOffset + index, value);
	}

	public char getChar(int offset)
	{
		boundsCheck(offset, SIZE_OF_CHAR);
		return UNSAFE.getChar(byteArray, addressOffset + offset);
	}

	public void putChar(int index, char value)
	{
		boundsCheck(index, SIZE_OF_INT);
		UNSAFE.putInt(byteArray, addressOffset + index, value);
	}
}