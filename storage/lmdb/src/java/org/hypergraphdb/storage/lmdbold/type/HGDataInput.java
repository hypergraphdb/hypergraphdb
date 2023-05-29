package org.hypergraphdb.storage.lmdb.type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public interface HGDataInput {
  // --- begin ByteArrayInputStream compatible methods ---
	public int available();

	public boolean markSupported();

	public void mark(int readLimit);

	public void reset();

	public long skip(long count);

	public int read();

	public int read(byte[] toBuf);

	public int read(byte[] toBuf, int offset, int length);
	// --- end ByteArrayInputStream compatible methods ---

	/**
	 * Equivalent to <code>skip()<code> but takes an int parameter instead of a
	 * long, and does not check whether the count given is larger than the
	 * number of remaining bytes.
	 * 
	 * @see #skip(long)
	 */
	public void skipFast(int count);

	/**
	 * Equivalent to <code>read()<code> but does not throw
	 * <code>IOException</code>.
	 * 
	 * @see #read()
	 */
	public int readFast();

	/**
	 * Equivalent to <code>read(byte[])<code> but does not throw
	 * <code>IOException</code>.
	 * 
	 * @see #read(byte[])
	 */
	public int readFast(byte[] toBuf);

	/**
	 * Equivalent to <code>read(byte[],int,int)<code> but does not throw
	 * <code>IOException</code>.
	 * 
	 * @see #read(byte[],int,int)
	 */
	public int readFast(byte[] toBuf, int offset, int length);

	/**
	 * Returns the underlying data being read.
	 * 
	 * @return the underlying data.
	 */
	public byte[] getBufferBytes();

	/**
	 * Returns the offset at which data is being read from the buffer.
	 * 
	 * @return the offset at which data is being read.
	 */
	public int getBufferOffset();

	/**
	 * Returns the end of the buffer being read.
	 * 
	 * @return the end of the buffer.
	 */
	public int getBufferLength();	
	
	
	// --- begin DataInput compatible methods ---

	/**
	 * Reads a null-terminated UTF string from the data buffer and converts the data from UTF to Unicode. Reads
	 * values that were written using {@link TupleOutput#writeString(String)}.
	 * 
	 * @return the converted string.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if no null terminating byte is found in the buffer.
	 * 
	 * @throws IllegalArgumentException
	 *           malformed UTF data is encountered.
	 * 
	 * @see <a href="package-summary.html#stringFormats">String Formats</a>
	 */
	public String readString() throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Reads a char (two byte) unsigned value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeChar}. 
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public char readChar() throws IndexOutOfBoundsException;

	/**
	 * Reads a boolean (one byte) unsigned value from the buffer and returns true if it is non-zero and false if
	 * it is zero. Reads values that were written using {@link TupleOutput#writeBoolean}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public boolean readBoolean() throws IndexOutOfBoundsException;

	/**
	 * Reads a signed byte (one byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeByte}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public byte readByte() throws IndexOutOfBoundsException;

	/**
	 * Reads a signed short (two byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeShort}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public short readShort() throws IndexOutOfBoundsException;

	/**
	 * Reads a signed int (four byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeInt}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int readInt() throws IndexOutOfBoundsException;

	/**
	 * Reads a signed long (eight byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeLong}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public long readLong() throws IndexOutOfBoundsException;

	/**
	 * Reads an unsorted float (four byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeFloat}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#floatFormats">Floating Point Formats</a>
	 */
	public float readFloat() throws IndexOutOfBoundsException;

	/**
	 * Reads an unsorted double (eight byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeDouble}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#floatFormats">Floating Point Formats</a>
	 */
	public double readDouble() throws IndexOutOfBoundsException;

	/**
	 * Reads a sorted float (four byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeSortedFloat}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#floatFormats">Floating Point Formats</a>
	 */
	public float readSortedFloat() throws IndexOutOfBoundsException;

	/**
	 * Reads a sorted double (eight byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeSortedDouble}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#floatFormats">Floating Point Formats</a>
	 */
	public double readSortedDouble() throws IndexOutOfBoundsException;

	/**
	 * Reads an unsigned byte (one byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeUnsignedByte}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int readUnsignedByte() throws IndexOutOfBoundsException;

	/**
	 * Reads an unsigned short (two byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeUnsignedShort}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int readUnsignedShort() throws IndexOutOfBoundsException;

	// --- end DataInput compatible methods ---

	/**
	 * Reads an unsigned int (four byte) value from the buffer. Reads values that were written using
	 * {@link TupleOutput#writeUnsignedInt}.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public long readUnsignedInt() throws IndexOutOfBoundsException;

	/**
	 * Reads the specified number of bytes from the buffer, converting each unsigned byte value to a character
	 * of the resulting string. Reads values that were written using {@link TupleOutput#writeBytes}.
	 * 
	 * @param length
	 *          is the number of bytes to be read.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public String readBytes(int length) throws IndexOutOfBoundsException;

	/**
	 * Reads the specified number of characters from the buffer, converting each two byte unsigned value to a
	 * character of the resulting string. Reads values that were written using {@link TupleOutput#writeChars}.
	 * 
	 * @param length
	 *          is the number of characters to be read.
	 * 
	 * @return the value read from the buffer.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public String readChars(int length) throws IndexOutOfBoundsException;

	/**
	 * Reads the specified number of bytes from the buffer, converting each unsigned byte value to a character
	 * of the resulting array. Reads values that were written using {@link TupleOutput#writeBytes}.
	 * 
	 * @param chars
	 *          is the array to receive the data and whose length is used to determine the number of bytes to be
	 *          read.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public void readBytes(char[] chars) throws IndexOutOfBoundsException;

	/**
	 * Reads the specified number of characters from the buffer, converting each two byte unsigned value to a
	 * character of the resulting array. Reads values that were written using {@link TupleOutput#writeChars}.
	 * 
	 * @param chars
	 *          is the array to receive the data and whose length is used to determine the number of characters
	 *          to be read.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if not enough bytes are available in the buffer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public void readChars(char[] chars) throws IndexOutOfBoundsException;

	/**
	 * Reads the specified number of UTF characters string from the data buffer and converts the data from UTF
	 * to Unicode. Reads values that were written using {@link TupleOutput#writeString(char[])}.
	 * 
	 * @param length
	 *          is the number of characters to be read.
	 * 
	 * @return the converted string.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if no null terminating byte is found in the buffer.
	 * 
	 * @throws IllegalArgumentException
	 *           malformed UTF data is encountered.
	 * 
	 * @see <a href="package-summary.html#stringFormats">String Formats</a>
	 */
	public String readString(int length) throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Reads the specified number of UTF characters string from the data buffer and converts the data from UTF
	 * to Unicode. Reads values that were written using {@link TupleOutput#writeString(char[])}.
	 * 
	 * @param chars
	 *          is the array to receive the data and whose length is used to determine the number of characters
	 *          to be read.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if no null terminating byte is found in the buffer.
	 * 
	 * @throws IllegalArgumentException
	 *           malformed UTF data is encountered.
	 * 
	 * @see <a href="package-summary.html#stringFormats">String Formats</a>
	 */
	public void readString(char[] chars) throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Returns the byte length of a null-terminated UTF string in the data buffer, including the terminator.
	 * Used with string values that were written using {@link TupleOutput#writeString(String)}.
	 * 
	 * @throws IndexOutOfBoundsException
	 *           if no null terminating byte is found in the buffer.
	 * 
	 * @throws IllegalArgumentException
	 *           malformed UTF data is encountered.
	 * 
	 * @see <a href="package-summary.html#stringFormats">String Formats</a>
	 */
	public int getStringByteLength() throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Reads an unsorted packed integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int readPackedInt(); 

	/**
	 * Returns the byte length of a packed integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int getPackedIntByteLength();

	/**
	 * Reads an unsorted packed long integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public long readPackedLong();

	/**
	 * Returns the byte length of a packed long integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int getPackedLongByteLength();

	/**
	 * Reads a sorted packed integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int readSortedPackedInt();

	/**
	 * Returns the byte length of a sorted packed integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int getSortedPackedIntByteLength();

	/**
	 * Reads a sorted packed long integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public long readSortedPackedLong();

	/**
	 * Returns the byte length of a sorted packed long integer.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int getSortedPackedLongByteLength();

	/**
	 * Reads a {@code BigInteger}.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public BigInteger readBigInteger();

	/**
	 * Returns the byte length of a {@code BigInteger}.
	 * 
	 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
	 */
	public int getBigIntegerByteLength();

	/**
	 * Reads an unsorted {@code BigDecimal}.
	 * 
	 * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal Formats</a>
	 */
	public BigDecimal readBigDecimal();

	/**
	 * Returns the byte length of an unsorted {@code BigDecimal}.
	 * 
	 * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal Formats</a>
	 */
	public int getBigDecimalByteLength();

	/**
	 * Reads a sorted {@code BigDecimal}, with support for correct default sorting.
	 * 
	 * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal Formats</a>
	 */
	public BigDecimal readSortedBigDecimal();    

	/**
	 * Returns the byte length of a sorted {@code BigDecimal}.
	 * 
	 * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal Formats</a>
	 */
	public int getSortedBigDecimalByteLength();
	
	
  /** Read an {@code sint32} field value from the stream. */
  public int readSInt32();

  /** Read an {@code sint64} field value from the stream. */
  public long readSInt64();

  /**
   * Read a raw Varint from the stream.  If larger than 32 bits, discard the
   * upper bits.
   */
  public int readRawVarint32();

  /** Read a raw Varint from the stream. */
  public long readRawVarint64();
  
  public int readRawVarint30();

  public long readRawVarint59();
}