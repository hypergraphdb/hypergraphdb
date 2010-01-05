/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.storage.ByteArrayConverter;
import java.util.Comparator;

/**
 * <p>
 * A <code>HGPrimitiveType</code> is a <code>HGAtomType</code> that
 * store its values directly as raw byte data.
 * </p>
 * 
 * <p>
 * A primitive type must expose those operations to HyperGraph in order
 * to facilitate indexing and other storage management, and optimization
 * activities. The exposed conversion of a primitive type to/from a byte
 * buffer are defined in the parent <code>ByteArrayConverter</code> interface.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGPrimitiveType<JavaType> extends HGAtomType, ByteArrayConverter<JavaType>
{
	/**
	 * Return a <code>java.util.Comparator</code> instance that provides 
	 * an order relation of the values of the primitive type. An implementation
	 * is allowed to return <code>null</code> in which case it is assumed 
	 * that the type does not offer an ordering relation. However, if a 
	 * non-null value is returned, it is must be of a publicly available
	 * and default constructible class.
	 */
	Comparator<byte[]> getComparator();
}
