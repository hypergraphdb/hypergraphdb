package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.lmdb.type.HGDataInput;
import org.hypergraphdb.storage.lmdb.type.HGDataOutput;
import org.hypergraphdb.storage.lmdb.type.TupleInput;

public class LmdbDataInput extends TupleInput implements HGDataInput {
  public LmdbDataInput(final byte[] buffer) {
    super(buffer);
  }
	
  public LmdbDataInput(final byte[] buffer, int offset, int length) {
    super(buffer, offset, length);
  }
	
	/**
	 * Creates a data input object for reading a byte array of tuple data. A reference to the byte array will
	 * be kept by this object (it will not be copied) and therefore the byte array should not be modified while
	 * this object is in use.
	 * 
	 * @param buffer
	 *          is the byte array to be read and should contain data in tuple format.
	 */
  public static HGDataInput getInstance(byte[] buffer) {
  	LmdbDataInput di = new LmdbDataInput(buffer);
  	return di;
  }

  /**
   * Creates a tuple input object for reading a byte array of tuple data at
   * a given offset for a given length.  A reference to the byte array will
   * be kept by this object (it will not be copied) and therefore the byte
   * array should not be modified while this object is in use.
   *
   * @param buffer is the byte array to be read and should contain data in
   * tuple format.
   *
   * @param offset is the byte offset at which to begin reading.
   *
   * @param length is the number of bytes to be read.
   */
  public static HGDataInput getInstance(byte[] buffer, int offset, int length) {
  	LmdbDataInput di = new LmdbDataInput(buffer, offset, length);
  	return di;
  }

  /**
   * Creates a tuple input object from the data contained in a tuple output
   * object.  A reference to the tuple output's byte array will be kept by
   * this object (it will not be copied) and therefore the tuple output
   * object should not be modified while this object is in use.
   *
   * @param output is the tuple output object containing the data to be read.
   */
  public static HGDataInput getInstance(HGDataOutput output) {
  	LmdbDataInput di = new LmdbDataInput(output.getBufferBytes(), output.getBufferOffset(), output.getBufferLength());
  	return di;
  }

  public final byte readFastByte() {
  	if (off >= len) {
  		throw new IllegalStateException("BufferOverflow");
  	}
  	
    return (buf[off++]);
  }
  
	/**
	 * This method is private since an unsigned long cannot be treated as such in Java, nor converted to a
	 * BigInteger of the same value.
	 */
	private final long readUnsignedLong() throws IndexOutOfBoundsException { //Needed to recopy here since private
		long c1 = readFast();
		long c2 = readFast();
		long c3 = readFast();
		long c4 = readFast();
		long c5 = readFast();
		long c6 = readFast();
		long c7 = readFast();
		long c8 = readFast();
		if ((c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8) < 0) {
			throw new IndexOutOfBoundsException();
		}
		return ((c1 << 56) | (c2 << 48) | (c3 << 40) | (c4 << 32) | (c5 << 24) | (c6 << 16) | (c7 << 8) | c8);
	}

  /** Read an {@code sint32} field value from the stream. */
	@Override
  public int readSInt32() {
    return decodeZigZag32(readRawVarint32());
  }

  /** Read an {@code sint64} field value from the stream. */
	@Override
  public long readSInt64() {
    return decodeZigZag64(readRawVarint64());
  }

  /**
   * Decode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 32-bit integer, stored in a signed int because
   *          Java has no explicit unsigned support.
   * @return A signed 32-bit integer.
   */
  public static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  /**
   * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 64-bit integer, stored in a signed int because
   *          Java has no explicit unsigned support.
   * @return A signed 64-bit integer.
   */
  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  
  /**
   * Read a raw Varint from the stream.  If larger than 32 bits, discard the
   * upper bits.
   */
	@Override
  public int readRawVarint32() {
    byte tmp = readFastByte();
    if (tmp >= 0) {
      return tmp;
    }
    
    int result = tmp & 0x7f;

    if ((tmp = readFastByte()) >= 0) {
      result |= tmp << 7;
    } 
    else {
      result |= (tmp & 0x7f) << 7;
    
      if ((tmp = readFastByte()) >= 0) {
        result |= tmp << 14;
      } 
      else {
        result |= (tmp & 0x7f) << 14;
       
        if ((tmp = readFastByte()) >= 0) {
          result |= tmp << 21;
        } 
        else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = readFastByte()) << 28;
        
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (readFastByte() >= 0) {
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
  public long readRawVarint64() {
    int shift = 0;
    long result = 0;

    while (shift < 64) {
      final byte b = readFastByte();
      result |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new HGException("Malformed Varint.");
  }
	
	@Override
  public int readRawVarint30() {
    byte tmp = readFastByte();
    int result = tmp & 0xff;  //byte 1
    
    if ((tmp = readFastByte()) >= 0) {  //byte 2
      result |= tmp << 8;
    } 
    else {
      result |= (tmp & 0x7f) << 8;  //byte 2
    
      if ((tmp = readFastByte()) >= 0) {  //byte 3
        result |= tmp << 15;
      } 
      else {
        result |= (tmp & 0x7f) << 15;  //byte 3

        result |= (tmp = readFastByte()) << 22;
      }
    }
		return result;
	}

	@Override
  public long readRawVarint59() {
    byte tmp;
    long result = 0;

    if ((tmp = readFastByte()) >= 0) { //byte 1
    	result = tmp & 0xffL;
    } 
    else {
      result |= (tmp & 0x7fL);

	    tmp = readFastByte(); //byte 2
	    result |= (tmp & 0xffL) << 7;
	    
	    tmp = readFastByte(); //byte 3
	    result |= (tmp & 0xffL) << 15;
	
	    if ((tmp = readFastByte()) >= 0) { //byte 4
	      result |= (tmp & 0xffL) << 23;
	    } 
	    else {
	      result |= (tmp & 0x7fL) << 23;
	    
	      if ((tmp = readFastByte()) >= 0) { //byte 5
	        result |= (tmp & 0xffL) << 30;
	      } 
	      else {
	        result |= (tmp & 0x7fL) << 30;
	
	        if ((tmp = readFastByte()) >= 0) { //byte 6
	          result |= (tmp & 0xffL) << 37;
	        } 
	        else {
	          result |= (tmp & 0x7fL) << 37;
	         
	          if ((tmp = readFastByte()) >= 0) { //byte 7
	            result |= (tmp & 0xffL) << 44;
	          } 
	          else {
	            result |= (tmp & 0x7fL) << 44;
	      	
	            tmp = readFastByte(); //byte 8
	            result |= (tmp & 0xffL) << 51;
	          }
	        }
	      }
      }
    }
		
    return result;
	}
}
