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
		final byte[] rnd = new byte[SIZE];        
		rndGenerator.nextBytes(rnd);        
		uuid = new UUID(UUID.TYPE_RANDOM_BASED, rnd);    	    	
	}

	private UUIDPersistentHandle(final UUID uuid)
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
	private UUIDPersistentHandle(final byte [] value, final int offset)
	{
		if (value == null) {
			throw new IllegalArgumentException("Attempt to construct UUIDPersistentHandle with a null value.");
		}
		else if (value.length - offset < SIZE) {
			throw new IllegalArgumentException("Attempt to construct UUIDPersistentHandle with wrong size byte array.");
		}
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
	public static UUIDPersistentHandle makeHandle(final byte [] value)
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
	public static UUIDPersistentHandle makeHandle(final byte [] value, final int offset)
	{
		return new UUIDPersistentHandle(value, offset);
	}

	/**
	 * <p>Construct from an existing UUID.</p>
	 * 
	 * @param value A UTF-8 encoded string representation of the UUID
	 */
	public static UUIDPersistentHandle makeHandle(final String value)
	{
		return new UUIDPersistentHandle(new UUID(value));
	}

	/**
	 * <p>Return a <code>byte [] </code> representation of this <code>UUIDPersistentHandle</code>.</p>
	 */
	@Override
	public byte [] toByteArray()
	{
		return uuid.mId;
	}

	public UUID getUuid()
	{
		return uuid;
	}


	public void setUuid(final UUID uuid)
	{
		this.uuid = uuid;
	}

	@Override
	public boolean equals(final Object other)
	{
		if (other == this) {
			return true;
		}
		else if (other instanceof UUIDPersistentHandle) {
			return uuid.equals(((UUIDPersistentHandle)other).uuid);
		}
		else if (other instanceof HGLiveHandle) {
			return ((HGLiveHandle)other).getPersistent().equals(this);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode()
	{
		return uuid.hashCode();
	}

	@Override
  public String toString()
  {
      return uuid.toString();
  }

  public String toStringValue()
  {
      return uuid.toString();
  }

	public int compareTo(HGHandle other)
	{
		if (other instanceof HGLiveHandle) {
			other = ((HGLiveHandle)other).getPersistent();
		}
		return uuid.compareTo(((UUIDPersistentHandle)other).uuid);
	}

	@Override
	public int compareTo(final HGPersistentHandle other)
	{
		return uuid.compareTo(((UUIDPersistentHandle)other).uuid);    	
	}


	@Override
	public HGPersistentHandle getPersistent() {
		return this;
	}    
}