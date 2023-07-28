package org.hypergraphdb.storage.mdbx;

public class MdbxDataOutput extends DataOutput implements HGDataOutput
{
	private MdbxDataOutput()
	{
		super();
	}

	private MdbxDataOutput(byte[] buffer)
	{
		super(buffer);
	}

	/**
	 * Creates a data output object for writing a byte array of tuple data.
	 */
	public static HGDataOutput getInstance()
	{
		return new MdbxDataOutput();
	}

	public static HGDataOutput getInstance(MdbxStorageImplementation store)
	{
		MdbxDataOutput instance = (MdbxDataOutput) getInstance();
		return instance;
	}

	/**
	 * Creates a data output object for writing a byte array of tuple data,
	 * using a given buffer. A new buffer will be allocated only if the number
	 * of bytes needed is greater than the length of this buffer. A reference to
	 * the byte array will be kept by this object and therefore the byte array
	 * should not be modified while this object is in use.
	 * 
	 * @param buffer
	 *            is the byte array to use as the buffer.
	 */
	public static HGDataOutput getInstance(byte[] buffer)
	{
		return new MdbxDataOutput(buffer);
	}

	public static HGDataOutput getInstance(MdbxStorageImplementation store,
			byte[] buffer)
	{
		MdbxDataOutput instance = (MdbxDataOutput) getInstance(buffer);
		return instance;
	}

	@Override
	public HGDataOutput writeBytes(String val)
	{
		return super.writeBytes(val.toCharArray());
	}

	@Override
	public HGDataOutput writeString(String val)
	{
		if (val != null)
		{
			char[] charArray = val.toCharArray();
			return super.writeString(charArray);
		}
		else
		{
			writeRawVarint32(0); // 0 is for null
			return this;
		}
	}

	@Override
	public HGDataOutput writeChar(int val)
	{
		return super.writeChar(val);
	}

	@Override
	public HGDataOutput writeChars(char[] val)
	{
		return super.writeChars(val);
	}

	@Override
	public HGDataOutput writeBoolean(boolean val)
	{
		return super.writeBoolean(val);
	}

	@Override
	public HGDataOutput writeBooleans(boolean[] val)
	{
		return super.writeBooleans(val);
	}

	@Override
	public HGDataOutput writeByte(int val)
	{
		return super.writeByte(val);
	}

	@Override
	public HGDataOutput writeBytes(byte[] val)
	{
		return super.writeBytes(val);
	}

	@Override
	public HGDataOutput writeShort(int val)
	{
		return super.writeShort(val);
	}

	@Override
	public HGDataOutput writeShorts(short[] val)
	{
		return super.writeShorts(val);
	}

	@Override
	public HGDataOutput writeInt(int val)
	{
		return super.writeInt(val);
	}

	@Override
	public HGDataOutput writeInts(int[] val)
	{
		return super.writeInts(val);
	}

	@Override
	public HGDataOutput writeLong(long val)
	{
		return super.writeLong(val);
	}

	@Override
	public HGDataOutput writeLongs(long[] val)
	{
		return super.writeLongs(val);
	}

	@Override
	public HGDataOutput writeFloat(float val)
	{
		return super.writeFloat(val);
	}

	@Override
	public HGDataOutput writeFloats(float[] val)
	{
		return super.writeFloats(val);
	}

	@Override
	public HGDataOutput writeDouble(double val)
	{
		return super.writeDouble(val);
	}

	@Override
	public HGDataOutput writeDoubles(double[] val)
	{
		return super.writeDoubles(val);
	}
}