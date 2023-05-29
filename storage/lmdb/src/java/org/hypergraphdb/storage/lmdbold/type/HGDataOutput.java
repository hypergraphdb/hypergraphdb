package org.hypergraphdb.storage.lmdb.type;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public interface HGDataOutput {
  // --- begin DataOutput compatible methods ---

  /**
   * Writes the specified bytes to the buffer, converting each character to
   * an unsigned byte value.
   * Writes values that can be read using {@link TupleInput#readBytes}.
   *
   * @param val is the string containing the values to be written.
   * Only characters with values below 0x100 may be written using this
   * method, since the high-order 8 bits of all characters are discarded.
   *
   * @return this tuple output object.
   *
   * @throws NullPointerException if the val parameter is null.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeBytes(String val);

  /**
   * Writes the specified characters to the buffer, converting each character
   * to a two byte unsigned value.
   * Writes values that can be read using {@link TupleInput#readChars}.
   *
   * @param val is the string containing the characters to be written.
   *
   * @return this tuple output object.
   *
   * @throws NullPointerException if the val parameter is null.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeChars(String val);

  /**
   * Writes the specified characters to the buffer, converting each character
   * to UTF format, and adding a null terminator byte.
   * Writes values that can be read using {@link TupleInput#readString()}.
   *
   * @param val is the string containing the characters to be written.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#stringFormats">String Formats</a>
   */
  public HGDataOutput writeString(String val);

  /**
   * Writes a char (two byte) unsigned value to the buffer.
   * Writes values that can be read using {@link TupleInput#readChar}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeChar(int val);

  /**
   * Writes a char (two byte) unsigned value to the buffer.
   * Writes values that can be read using {@link TupleInput#readChar}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeChar(Character val);

  /**
   * Writes a boolean (one byte) unsigned value to the buffer, writing one
   * if the value is true and zero if it is false.
   * Writes values that can be read using {@link TupleInput#readBoolean}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeBoolean(boolean val);

  /**
   * Writes an signed byte (one byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readByte}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeByte(int val);

  /**
   * Writes an signed short (two byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readShort}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeShort(int val);

  /**
   * Writes an signed int (four byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readInt}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeInt(int val);

  /**
   * Writes an signed long (eight byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readLong}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeLong(long val);

  /**
   * Writes an unsorted float (four byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readFloat}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#floatFormats">Floating Point
   * Formats</a>
   */
  public HGDataOutput writeFloat(float val);

  /**
   * Writes an unsorted double (eight byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readDouble}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#floatFormats">Floating Point
   * Formats</a>
   */
  public HGDataOutput writeDouble(double val);

  /**
   * Writes a sorted float (four byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readSortedFloat}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#floatFormats">Floating Point
   * Formats</a>
   */
  public HGDataOutput writeSortedFloat(float val);

  /**
   * Writes a sorted double (eight byte) value to the buffer.
   * Writes values that can be read using {@link TupleInput#readSortedDouble}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#floatFormats">Floating Point
   * Formats</a>
   */
  public HGDataOutput writeSortedDouble(double val);

  // --- end DataOutput compatible methods ---

  /**
   * Writes the specified bytes to the buffer, converting each character to
   * an unsigned byte value.
   * Writes values that can be read using {@link TupleInput#readBytes}.
   *
   * @param chars is the array of values to be written.
   * Only characters with values below 0x100 may be written using this
   * method, since the high-order 8 bits of all characters are discarded.
   *
   * @return this tuple output object.
   *
   * @throws NullPointerException if the chars parameter is null.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeBytes(char[] chars);

  /**
   * Writes the specified characters to the buffer, converting each character
   * to a two byte unsigned value.
   * Writes values that can be read using {@link TupleInput#readChars}.
   *
   * @param chars is the array of characters to be written.
   *
   * @return this tuple output object.
   *
   * @throws NullPointerException if the chars parameter is null.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeChars(char[] chars);

  /**
   * Writes the specified characters to the buffer, converting each character
   * to UTF format.
   * Writes values that can be read using {@link TupleInput#readString(int)}
   * or {@link TupleInput#readString(char[])}.
   *
   * @param chars is the array of characters to be written.
   *
   * @return this tuple output object.
   *
   * @throws NullPointerException if the chars parameter is null.
   *
   * @see <a href="package-summary.html#stringFormats">String Formats</a>
   */
  public HGDataOutput writeString(char[] chars);

  /**
   * Writes an unsigned byte (one byte) value to the buffer.
   * Writes values that can be read using {@link
   * TupleInput#readUnsignedByte}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeUnsignedByte(int val);

  /**
   * Writes an unsigned short (two byte) value to the buffer.
   * Writes values that can be read using {@link
   * TupleInput#readUnsignedShort}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeUnsignedShort(int val);

  /**
   * Writes an unsigned int (four byte) value to the buffer.
   * Writes values that can be read using {@link
   * TupleInput#readUnsignedInt}.
   *
   * @param val is the value to write to the buffer.
   *
   * @return this tuple output object.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeUnsignedInt(long val);

  /**
   * Writes an unsorted packed integer.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writePackedInt(int val);

  /**
   * Writes an unsorted packed long integer.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writePackedLong(long val);

  /**
   * Writes a sorted packed integer.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeSortedPackedInt(int val);

  /**
   * Writes a sorted packed long integer.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeSortedPackedLong(long val);

  /**
   * Writes a {@code BigInteger}.
   *
   * @throws NullPointerException if val is null.
   *
   * @throws IllegalArgumentException if the byte array representation of val
   * is larger than 0x7fff bytes.
   *
   * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
   */
  public HGDataOutput writeBigInteger(BigInteger val);

  /**
   * Writes an unsorted {@code BigDecimal}.
   *
   * @throws NullPointerException if val is null.
   *
   * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal
   * Formats</a>
   */
  public HGDataOutput writeBigDecimal(BigDecimal val);
  
  /**
   * Writes a sorted {@code BigDecimal}.
   *
   * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal
   * Formats</a>
   */
  public HGDataOutput writeSortedBigDecimal(BigDecimal val);
  
  // --- begin ByteArrayOutputStream compatible methods ---
  public int size();

  public void reset();

  public void write(int b);

  public void write(byte[] fromBuf);

  public void write(byte[] fromBuf, int offset, int length);

  public void writeTo(OutputStream out) throws IOException;

  public String toString();

  public String toString(String encoding) throws UnsupportedEncodingException;

  public byte[] toByteArray();
  // --- end ByteArrayOutputStream compatible methods ---

  /**
   * Equivalent to <code>write(int)<code> but does not throw
   * <code>IOException</code>.
   * @see #write(int)
   */
  public void writeFast(int b);

  /**
   * Equivalent to <code>write(byte[])<code> but does not throw
   * <code>IOException</code>.
   * @see #write(byte[])
   */
  public void writeFast(byte[] fromBuf);

  /**
   * Equivalent to <code>write(byte[],int,int)<code> but does not throw
   * <code>IOException</code>.
   * @see #write(byte[],int,int)
   */
  public void writeFast(byte[] fromBuf, int offset, int length);

  /**
   * Returns the buffer owned by this object.
   *
   * @return the buffer.
   */
  public byte[] getBufferBytes();

  /**
   * Returns the offset of the internal buffer.
   *
   * @return always zero currently.
   */
  public int getBufferOffset();

  /**
   * Returns the length used in the internal buffer, i.e., the offset at
   * which data will be written next.
   *
   * @return the buffer length.
   */
  public int getBufferLength();

  /**
   * Ensure that at least the given number of bytes are available in the
   * internal buffer.
   *
   * @param sizeNeeded the number of bytes desired.
   */
  public void makeSpace(int sizeNeeded);

  /**
   * Skip the given number of bytes in the buffer.
   *
   * @param sizeAdded number of bytes to skip.
   */
  public void addSize(int sizeAdded);
  
  
  /** Write an {@code sint32} field to the stream. */
  public void writeSInt32(final int value);

  /** Write an {@code sint64} field to the stream. */
  public void writeSInt64(final long value);

  /**
   * Encode and write a varint.  {@code value} is treated as
   * unsigned, so it won't be sign-extended if negative.
   */
  public void writeRawVarint32(int value);

  /** Encode and write a varint. */
  public void writeRawVarint64(long value);
  
	/**
	 * Encode and write a varint30. This is a special encoding supporting unsigned values up to 2^30 that are
	 * encoded in variable 2, 3 or 4 bytes representation. {@code value} is treated as unsigned, so it won't be
	 * sign-extended if negative.
	 */
  public void writeRawVarint30(final int val);
  
	/**
	 * Encode and write a varint59. This is a special encoding supporting unsigned values up to 2^59 that are
	 * encoded in variable 4 to 8 bytes representation. {@code value} is treated as unsigned, so it won't be
	 * sign-extended if negative.
	 */
  public void writeRawVarint59(final long val);

}