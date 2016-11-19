/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.io.InputStream;

/**
 * <p>
 * The <code>HGHandleFactory</code> is used to manage persistent handles
 * for the atoms of {@link HyperGraph} instance. One such factory is configured
 * per HyperGraphDB instance via the {@link HGConfiguration} passed when the instance
 * is opened. The factory is responsible for creating new {@link HGPersistentHandle}s
 * that must be unique at least within the current database instance. Certain factories
 * can also guarantee universal uniqueness (e.g. in a distributed environment or across
 * the Internet). Factories are also responsible for creating handle out of their
 * <code>String</code> or <code>byte[]</code> representations. The main requirement that 
 * a handle factory implementation must observe is that all
 * persistent handles must be of the exact same <code>byte[]</code> size. For example, this
 * rules out the use of URIs as persistent handles.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGHandleFactory
{
    /**
     * <p>Construct and return a new, unique persistent handle. The handle 
     * is permanently unique. It can be persistent (serialized) and restored without
     * ever conflicting with another handle. 
     * </p>
     */
    HGPersistentHandle makeHandle();    
    /**
     * <p>Construct a persistent handle from its string representation.</p>
     */
    HGPersistentHandle makeHandle(String handleAsString);
    
    /**
     * <p>Construct a persistent handle from its byte array representation.</p>
     * 
     * @param buffer The byte array holding the handle value.
     */
    HGPersistentHandle makeHandle(byte [] buffer);

    /**
     * <p>Construct a persistent handle from its byte array representation where the byte array
     * is part of a larger buffer and located at a particular offset.</p>
     * 
     * @param buffer The byte array holding the handle value.
     * @param offset The offset within <code>buffer</code> where the handle value starts.
     */
    HGPersistentHandle makeHandle(byte [] buffer, int offset);
    
    /**
     * <p>
     * Construct a persistent handle by reading the next input from an input stream. 
     * </p>
     * 
     * @param in An input stream in a valid state. Implementations assume that there is
     * data available and that the data is well formed representing a handle. 
     * @return The newly read {@link HGPersistentHandle}. The stream will naturally be 
     * advanced. 
     */
    HGPersistentHandle makeHandle(InputStream in);
    
    /**
     * <p>Return the representation of a null persistent handle. A null
     * handle is a single instance and therefore can be compared for equality by
     * doing direct Java object reference comparison. A null handle can be 
     * recorded in storage as any other persistent handle - it refers to no
     * value. Not that while HyperGraph's type system does not provide 
     * for <code>null</code> atom values or value projections, null references
     * to values are permitted and supported through the null 
     * <code>HGPersistentHandle</code></p>
     */
    HGPersistentHandle nullHandle();
    
    /**
     * <p>
     * The <code>anyHandle</code> is a persistent handle constant that represents
     * a "don't care" handle during querying and comparison operations. It can be used, for
     * instance, when defining an <code>OrdererLinkCondition</code> in a query. 
     * </p>
     * 
     * <p>
     * For efficiency reasons, the <code>equals</code> method of the various implementations of 
     * the <code>HGHandle</code> interface ignore this constant, even though it would be
     * more consistent to for <code>anyHandle.equals(x)</code> and <code>x.equals(anyHandle)</code>
     * to always return true.
     * </p>
     */        
    HGPersistentHandle anyHandle();
    
    HGPersistentHandle topTypeHandle();
    HGPersistentHandle nullTypeHandle();
    HGPersistentHandle linkTypeHandle();
    HGPersistentHandle subsumesTypeHandle();
}