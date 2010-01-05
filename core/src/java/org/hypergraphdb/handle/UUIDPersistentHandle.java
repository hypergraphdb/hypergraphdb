/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import java.security.SecureRandom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
//import java.util.UUID;

public final class UUIDPersistentHandle implements HGPersistentHandle
{
    static final long serialVersionUID = -1;    
    static final SecureRandom rndGenerator = new SecureRandom();
    
    private UUID uuid;
    
    /**
     * The number of bytes in the <code>byte []</code> representation
     * of a <code>UUIDPersistentHandle</code>. 
     */
    public static final int SIZE = 16;
    
    public static final UUIDPersistentHandle UUID_NULL_HANDLE = 
    	new UUIDPersistentHandle(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, 0);
        
    /**
     * <p>Default constructor create a new UUID.</p>
     */    
    public UUIDPersistentHandle()
    {
        byte[] rnd = new byte[16];        
        rndGenerator.nextBytes(rnd);        
        uuid = new UUID(UUID.TYPE_RANDOM_BASED, rnd);    	    	
    }

    
    private UUIDPersistentHandle(UUID uuid)
    {
        this.uuid = uuid;
    }
    
    /**
     * <p>Construct from an existing UUID.</p>
     * 
     * <p>This constructor is made private to allow for caching of handles. The
     * <code>makeHandle</code> static method should be called instead.
     * </p>
     * 
     * @param value An array of 16 bytes representing the UUID. If this parameter
     * is <code>null</code> or of size != 16, an <code>IllegalArgumentException</code>
     * is thrown.
     */
    private UUIDPersistentHandle(byte [] value, int offset)
    {
        if (value == null)
            throw new IllegalArgumentException("Attempt to construct UUIDPersistentHandle with a null value.");
        else if (value.length - offset < 16)
            throw new IllegalArgumentException("Attempt to construct UUIDPersistentHandle with wrong size byte array.");
/*        long msb = 0;
        long lsb = 0;
        for (int i=offset; i < 8 + offset; i++)
            msb = (msb << 8) | (value[i] & 0xff);
        for (int i=8+offset; i < 16 + offset; i++)
            lsb = (lsb << 8) | (value[i] & 0xff);
        uuid = new UUID(msb, lsb); */
        uuid = new UUID(value, offset);
    }
    
    /**
     * <p>Return the representation of a <code>nil</code> handle, that is a handle that does
     * not refer to anything.</p>
     */
    public static UUIDPersistentHandle nullHandle()
    {
    	return UUID_NULL_HANDLE;
    }
    
    /**
     * <p>Construct a brand new UUID-based handle. A UUID will be generated based
     * on the algorithm configured at application started.</p>
     */
    public static UUIDPersistentHandle makeHandle()
    {
        return new UUIDPersistentHandle();
    }
    
    /**
     * <p>Construct from an existing UUID.</p>
     * 
     * @param value An array of <code>UUIDPersistentHandle.SIZE</code> bytes representing the UUID. If this parameter
     * is <code>null</code> or of size != <code>UUIDPersistentHandle.SIZE</code>, an <code>IllegalArgumentException</code>
     * is thrown.
     */
    public static UUIDPersistentHandle makeHandle(byte [] value)
    {
        return new UUIDPersistentHandle(value, 0);
    }

    /**
     * <p>Construct from an existing UUID.</p>
     * 
     * @param value An array of <code>offset + UUIDPersistentHandle.SIZE</code> bytes representing the UUID. If this parameter
     * is <code>null</code> or of size < <code>offset + UUIDPersistentHandle.SIZE</code>, an <code>IllegalArgumentException</code>
     * is thrown.
     * @param offset The starting position in <code>value</code> of the data holding the 
     * <code>UUIDPersistentHandle</code> representation.
     */
    public static UUIDPersistentHandle makeHandle(byte [] value, int offset)
    {
        return new UUIDPersistentHandle(value, offset);
    }
    
    /**
     * <p>Construct from an existing UUID.</p>
     * 
     * @param value A UTF-8 encoded string representation of the UUID
     */
    public static UUIDPersistentHandle makeHandle(String value)
    {
    	return new UUIDPersistentHandle(new UUID(value));
        //return new UUIDPersistentHandle(UUID.fromString(value));
    }
    

    /**
     * <p>Return a <code>byte [] </code> representation of this <code>UUIDPersistentHandle</code>.</p>
     */
    public byte [] toByteArray()
    {
/*    	long msb = uuid.getMostSignificantBits();
    	long lsb = uuid.getLeastSignificantBits();
    	byte [] data = new byte[16]; // should we precompute and cache this?
        data[0] = (byte) ((msb >>> 56)); 
        data[1] = (byte) ((msb >>> 48));
        data[2] = (byte) ((msb >>> 40)); 
        data[3] = (byte) ((msb >>> 32));
        data[4] = (byte) ((msb >>> 24)); 
        data[5] = (byte) ((msb >>> 16));
        data[6] = (byte) ((msb >>> 8)); 
        data[7] = (byte) ((msb >>> 0));
        data[8] = (byte) ((lsb >>> 56)); 
        data[9] = (byte) ((lsb >>> 48));
        data[10] = (byte) ((lsb >>> 40)); 
        data[11] = (byte) ((lsb >>> 32));
        data[12] = (byte) ((lsb >>> 24)); 
        data[13] = (byte) ((lsb >>> 16));
        data[14] = (byte) ((lsb >>> 8)); 
        data[15] = (byte) ((lsb >>> 0));
        return data; */
    	return uuid.mId;
    }
    
    public boolean equals(Object other)
    {
        if (other == this)
        	return true;
        else if (other instanceof UUIDPersistentHandle)
        	return uuid.equals(((UUIDPersistentHandle)other).uuid);
        else if (other instanceof HGLiveHandle)
            return ((HGLiveHandle)other).getPersistentHandle().equals(this);
        else
        	return false;
    }
    
    public int hashCode()
    {
        return uuid.hashCode();
    }
    
    public String toString()
    {
        return uuid.toString();
    }
    
    public int compareTo(HGHandle other)
    {
    	if (other instanceof HGLiveHandle)
    		other = ((HGLiveHandle)other).getPersistentHandle();
    	return uuid.compareTo(((UUIDPersistentHandle)other).uuid);
    }
    
    public int compareTo(HGPersistentHandle other)
    {
    	return uuid.compareTo(((UUIDPersistentHandle)other).uuid);    	
    }    
}
