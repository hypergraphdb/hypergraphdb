/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.handle;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.safehaus.uuid.UUIDGenerator;
import org.safehaus.uuid.EthernetAddress;

public final class UUIDPersistentHandle implements HGPersistentHandle
{
    static final long serialVersionUID = -1;
    static EthernetAddress eAddress = null;
    
    static
    {
    	System.loadLibrary("EthernetAddress");
    	com.ccg.net.ethernet.EthernetAddress nativeEAddress = com.ccg.net.ethernet.EthernetAddress.getPrimaryAdapter();
/*    	if (nativeEAddress == null)
    		throw new Error(
    				"Unable to determine primary Ethernet Adapter. HyperGraphDB can only be used on machine where such information is available."); */
    	if (nativeEAddress != null)
    		eAddress = new EthernetAddress(nativeEAddress.getBytes());
    }
    
    private UUID uuid;
    
    /**
     * The number of bytes in the <code>byte []</code> representation
     * of a <code>UUIDPersistentHandle</code>. 
     */
    public static final int SIZE = 16;
    
    public static final UUIDPersistentHandle UUID_NULL_HANDLE = new UUIDPersistentHandle(UUID.valueOf(
    		new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0} ));
    
    /**
     * <p>Default constructor create a new UUID.</p>
     */
    private UUIDPersistentHandle()
    {
        // TODO - this is not the perfect way to do the generation, so it should be replaced
        // in a real production system.
    	if (eAddress != null)
    		uuid = new UUID(UUIDGenerator.getInstance().generateTimeBasedUUID(eAddress).asByteArray());
    	else
    		uuid = new UUID(UUIDGenerator.getInstance().generateTimeBasedUUID().asByteArray());
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
        else 
            uuid = UUID.valueOf(value, offset);
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
        return new UUIDPersistentHandle(UUID.valueOf(value));
    }
    

    /**
     * <p>Return a <code>byte [] </code> representation of this <code>UUIDPersistentHandle</code>.</p>
     */
    public byte [] toByteArray()
    {
        return uuid.toByteArray();
    }
    
    public boolean equals(Object other)
    {
        if (other == null)
        	return false;
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
    	UUIDPersistentHandle handle = (UUIDPersistentHandle)other;
    	return uuid.compareTo(handle.uuid);
    }
}