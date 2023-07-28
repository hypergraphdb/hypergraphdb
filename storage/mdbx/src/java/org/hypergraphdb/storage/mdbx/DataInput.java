package org.hypergraphdb.storage.mdbx;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.mdbx.type.TupleInput;
import org.hypergraphdb.storage.mdbx.type.util.UtfOps;

public abstract class DataInput extends TupleInput implements HGDataInput
{
	protected DataInput(final byte[] buffer)
	{
		super(buffer);
	}

	protected DataInput(final byte[] buffer, int offset, int length)
	{
		super(buffer, offset, length);
	}

	public final byte readFastByte()
	{
		if (off >= len)
		{
			throw new IllegalStateException("BufferOverflow");
		}

		return (buf[off++]);
	}

	@Override
	public String readString()
			throws IndexOutOfBoundsException, IllegalArgumentException
	{
		int lgth = readRawVarint32();

		if (lgth == 0)
		{
			return null;
		}
		if (lgth == 1)
		{
			return "";
		}

		byte[] myBuf = buf;
		int myOff = off;
		int byteLen = lgth - 1; // -1 since string use 0 for null and 1 for
								// empty
		skip(byteLen);
		return UtfOps.bytesToString(myBuf, myOff, byteLen);
	}

//	@Override
	public char[] readChars() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		char[] val = new char[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readChar();
		}

		return val;
	}

//	@Override
	public boolean[] readBooleans() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		boolean[] val = new boolean[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readBoolean();
		}

		return val;
	}

//	@Override
	public byte[] readBytes() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		byte[] val = new byte[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readByte();
		}

		return val;
	}

//	@Override
	public short[] readShorts() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		short[] val = new short[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readShort();
		}

		return val;
	}

//	@Override
	public int[] readInts() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		int[] val = new int[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readInt();
		}

		return val;
	}

//	@Override
	public long[] readLongs() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		long[] val = new long[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readLong();
		}

		return val;
	}

//	@Override
	public float[] readFloats() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		float[] val = new float[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readFloat();
		}

		return val;
	}

//	@Override
	public double[] readDoubles() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		double[] val = new double[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readDouble();
		}

		return val;
	}

//	@Override
	public float[] readSortedFloats() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		float[] val = new float[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readSortedFloat();
		}

		return val;
	}

//	@Override
	public double[] readSortedDoubles() throws IndexOutOfBoundsException
	{
		int lgth = readRawVarint32();
		double[] val = new double[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readSortedDouble();
		}

		return val;
	}

	/**
	 * This method is private since an unsigned long cannot be treated as such
	 * in Java, nor converted to a BigInteger of the same value.
	 */
	private final long readUnsignedLong() throws IndexOutOfBoundsException
	{ // Needed to recopy here since private
		long c1 = readFast();
		long c2 = readFast();
		long c3 = readFast();
		long c4 = readFast();
		long c5 = readFast();
		long c6 = readFast();
		long c7 = readFast();
		long c8 = readFast();
		if ((c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8) < 0)
		{
			throw new IndexOutOfBoundsException();
		}
		return ((c1 << 56) | (c2 << 48) | (c3 << 40) | (c4 << 32) | (c5 << 24)
				| (c6 << 16) | (c7 << 8) | c8);
	}

	/** Read an {@code sint32} field value from the stream. */
	@Override
	public int readSInt32()
	{
		return decodeZigZag32(readRawVarint32());
	}

	/** Read an {@code sint64} field value from the stream. */
	@Override
	public long readSInt64()
	{
		return decodeZigZag64(readRawVarint64());
	}

	/**
	 * Decode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into
	 * values that can be efficiently encoded with varint. (Otherwise, negative
	 * values must be sign-extended to 64 bits to be varint encoded, thus always
	 * taking 10 bytes on the wire.)
	 *
	 * @param n
	 *            An unsigned 32-bit integer, stored in a signed int because
	 *            Java has no explicit unsigned support.
	 * @return A signed 32-bit integer.
	 */
	public static int decodeZigZag32(final int n)
	{
		return (n >>> 1) ^ -(n & 1);
	}

	/**
	 * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
	 * values that can be efficiently encoded with varint. (Otherwise, negative
	 * values must be sign-extended to 64 bits to be varint encoded, thus always
	 * taking 10 bytes on the wire.)
	 *
	 * @param n
	 *            An unsigned 64-bit integer, stored in a signed int because
	 *            Java has no explicit unsigned support.
	 * @return A signed 64-bit integer.
	 */
	public static long decodeZigZag64(final long n)
	{
		return (n >>> 1) ^ -(n & 1);
	}

	/**
	 * Read a raw Varint from the stream. If larger than 32 bits, discard the
	 * upper bits.
	 */
	@Override
	public int readRawVarint32()
	{
		byte tmp = readFastByte();
		if (tmp >= 0)
		{
			return tmp;
		}

		int result = tmp & 0x7f;

		if ((tmp = readFastByte()) >= 0)
		{
			result |= tmp << 7;
		}
		else
		{
			result |= (tmp & 0x7f) << 7;

			if ((tmp = readFastByte()) >= 0)
			{
				result |= tmp << 14;
			}
			else
			{
				result |= (tmp & 0x7f) << 14;

				if ((tmp = readFastByte()) >= 0)
				{
					result |= tmp << 21;
				}
				else
				{
					result |= (tmp & 0x7f) << 21;
					result |= (tmp = readFastByte()) << 28;

					if (tmp < 0)
					{
						// Discard upper 32 bits.
						for (int i = 0; i < 5; i++)
						{
							if (readFastByte() >= 0)
							{
								return result;
							}
						}
						throw new HGException("Malformed Varint.");
					}
				}
			}
		}
		return result;
	}

	/** Read a raw Varint from the stream. */
	@Override
	public long readRawVarint64()
	{
		int shift = 0;
		long result = 0;

		while (shift < 64)
		{
			final byte b = readFastByte();
			result |= (long) (b & 0x7F) << shift;
			if ((b & 0x80) == 0)
			{
				return result;
			}
			shift += 7;
		}
		throw new HGException("Malformed Varint.");
	}

	@Override
	public int readRawVarint30()
	{
		byte tmp = readFastByte();
		int result = tmp & 0xff; // byte 1

		if ((tmp = readFastByte()) >= 0)
		{ // byte 2
			result |= tmp << 8;
		}
		else
		{
			result |= (tmp & 0x7f) << 8; // byte 2

			if ((tmp = readFastByte()) >= 0)
			{ // byte 3
				result |= tmp << 15;
			}
			else
			{
				result |= (tmp & 0x7f) << 15; // byte 3

				result |= (tmp = readFastByte()) << 22;
			}
		}
		return result;
	}

	@Override
	public long readRawVarint59()
	{
		byte tmp;
		long result = 0;

		if ((tmp = readFastByte()) >= 0)
		{ // byte 1
			result = tmp & 0xffL;
		}
		else
		{
			result |= (tmp & 0x7fL);

			tmp = readFastByte(); // byte 2
			result |= (tmp & 0xffL) << 7;

			tmp = readFastByte(); // byte 3
			result |= (tmp & 0xffL) << 15;

			if ((tmp = readFastByte()) >= 0)
			{ // byte 4
				result |= (tmp & 0xffL) << 23;
			}
			else
			{
				result |= (tmp & 0x7fL) << 23;

				if ((tmp = readFastByte()) >= 0)
				{ // byte 5
					result |= (tmp & 0xffL) << 30;
				}
				else
				{
					result |= (tmp & 0x7fL) << 30;

					if ((tmp = readFastByte()) >= 0)
					{ // byte 6
						result |= (tmp & 0xffL) << 37;
					}
					else
					{
						result |= (tmp & 0x7fL) << 37;

						if ((tmp = readFastByte()) >= 0)
						{ // byte 7
							result |= (tmp & 0xffL) << 44;
						}
						else
						{
							result |= (tmp & 0x7fL) << 44;

							tmp = readFastByte(); // byte 8
							result |= (tmp & 0xffL) << 51;
						}
					}
				}
			}
		}

		return result;
	}
}