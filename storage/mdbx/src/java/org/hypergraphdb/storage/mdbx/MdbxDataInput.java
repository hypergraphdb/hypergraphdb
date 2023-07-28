package org.hypergraphdb.storage.mdbx;

public class MdbxDataInput extends DataInput implements HGDataInput
{
	private MdbxStorageImplementation store;

	private MdbxDataInput(final byte[] buffer)
	{
		super(buffer);
	}

	private MdbxDataInput(final byte[] buffer, int offset, int length)
	{
		super(buffer, offset, length);
	}

	/**
	 * Creates a data input object for reading a byte array of tuple data. A
	 * reference to the byte array will be kept by this object (it will not be
	 * copied) and therefore the byte array should not be modified while this
	 * object is in use.
	 * 
	 * @param buffer
	 *            is the byte array to be read and should contain data in tuple
	 *            format.
	 */
	public static HGDataInput getInstance(byte[] buffer)
	{
		MdbxDataInput di = new MdbxDataInput(buffer);
		return di;
	}

	public static HGDataInput getInstance(MdbxStorageImplementation store, byte[] buffer)
	{
		return getInstance(store, buffer, 0, buffer.length);
	}

	/**
	 * Creates a tuple input object for reading a byte array of tuple data at a
	 * given offset for a given length. A reference to the byte array will be
	 * kept by this object (it will not be copied) and therefore the byte array
	 * should not be modified while this object is in use.
	 *
	 * @param buffer
	 *            is the byte array to be read and should contain data in tuple
	 *            format.
	 *
	 * @param offset
	 *            is the byte offset at which to begin reading.
	 *
	 * @param length
	 *            is the number of bytes to be read.
	 */
	public static HGDataInput getInstance(byte[] buffer, int offset, int length)
	{
		return new MdbxDataInput(buffer, offset, length);
	}

	public static HGDataInput getInstance(MdbxStorageImplementation store,
			byte[] buffer, int offset, int length)
	{
		MdbxDataInput instance = (MdbxDataInput) getInstance(buffer, offset,
				length);
		instance.store = store;
		return instance;
	}

	/**
	 * Creates a tuple input object from the data contained in a tuple output
	 * object. A reference to the tuple output's byte array will be kept by this
	 * object (it will not be copied) and therefore the tuple output object
	 * should not be modified while this object is in use.
	 *
	 * @param output
	 *            is the tuple output object containing the data to be read.
	 */
	public static HGDataInput getInstance(HGDataOutput output)
	{
		MdbxDataInput di = new MdbxDataInput(output.getBufferBytes(),
				output.getBufferOffset(), output.getBufferLength());
		return di;
	}


	private byte[] readUnsignedBytes(int lgth)
	{
		byte[] val = new byte[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = (byte) readUnsignedByte();
		}

		return val;
	}

	@Override
	public void readString(char[] chars)
			throws IndexOutOfBoundsException, IllegalArgumentException
	{
		super.readString(chars);
	}

	@Override
	public String readString()
			throws IndexOutOfBoundsException, IllegalArgumentException
	{
		return super.readString();
	}

	private char readSuperChar() throws IndexOutOfBoundsException
	{
		return super.readChar();
	}

	@Override
	public char readChar() throws IndexOutOfBoundsException
	{
		return super.readChar();
	}

	@Override
	public char[] readChars() throws IndexOutOfBoundsException
	{
		return super.readChars();
	}

	private boolean readSuperBoolean() throws IndexOutOfBoundsException
	{
		return super.readBoolean();
	}

	@Override
	public boolean readBoolean() throws IndexOutOfBoundsException
	{
		return super.readBoolean();
	}

	@Override
	public boolean[] readBooleans() throws IndexOutOfBoundsException
	{
		return super.readBooleans();
	}

	private byte readSuperByte() throws IndexOutOfBoundsException
	{
		return super.readByte();
	}

	@Override
	public byte readByte() throws IndexOutOfBoundsException
	{
		return super.readByte();
	}

	@Override
	public byte[] readBytes() throws IndexOutOfBoundsException
	{
		return super.readBytes();
	}

	private byte[] readRawBytes(int lgth)
	{
		byte[] val = new byte[lgth];

		for (int i = 0; i < val.length; i++)
		{
			val[i] = readByte();
		}

		return val;
	}

	private short readSuperShort() throws IndexOutOfBoundsException
	{
		return super.readShort();
	}

	@Override
	public short readShort() throws IndexOutOfBoundsException
	{
		return super.readShort();
	}

	@Override
	public short[] readShorts() throws IndexOutOfBoundsException
	{
		return super.readShorts();
	}

	private int readSuperInt() throws IndexOutOfBoundsException
	{
		return super.readInt();
	}

	@Override
	public int readInt() throws IndexOutOfBoundsException
	{
		return super.readInt();
	}

	@Override
	public int[] readInts() throws IndexOutOfBoundsException
	{
		return super.readInts();
	}

	private long readSuperLong() throws IndexOutOfBoundsException
	{
		return super.readLong();
	}

	@Override
	public long readLong() throws IndexOutOfBoundsException
	{
		return super.readLong();
	}

	@Override
	public long[] readLongs() throws IndexOutOfBoundsException
	{
		return super.readLongs();
	}

	private float readSuperFloat() throws IndexOutOfBoundsException
	{
		return super.readFloat();
	}

	@Override
	public float readFloat() throws IndexOutOfBoundsException
	{
		return super.readFloat();
	}

	@Override
	public float[] readFloats() throws IndexOutOfBoundsException
	{
		return super.readFloats();
	}

	private double readSuperDouble() throws IndexOutOfBoundsException
	{
		return super.readDouble();
	}

	@Override
	public double readDouble() throws IndexOutOfBoundsException
	{
		return super.readDouble();
	}

	@Override
	public double[] readDoubles() throws IndexOutOfBoundsException
	{
		return super.readDoubles();
	}
}