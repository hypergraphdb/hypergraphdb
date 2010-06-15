/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

/**
 * <p>
 * The <code>HGHandleFactory</code> class is used to manage persistent handles
 * for hypergraph atoms.
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
}