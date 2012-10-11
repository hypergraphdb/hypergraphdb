/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

/**
 * <p>
 * A utility interface to define mappings between <code>byte [] </code>
 * and object instances.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface ByteArrayConverter<T>
{
    byte [] toByteArray(T object);
    T fromByteArray(byte [] byteArray, int offset, int length);
}