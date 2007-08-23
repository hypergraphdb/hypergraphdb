/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

/**
 * <p>
 * The <code>HGLink</code> interface defines an unordered hypergraph link. A hypergraph link
 * is an atom that holds other atoms in a relationship. The precise semantics and interpretation
 * of the relationship are application specific and will generally depend on a particular link's
 * type and properties. The only restriction imposed by HyperGraph is that a link be of arity greater
 * than 0.
 * </p>
 * 
 * <p>
 * The collection of atoms which an instance of <code>HGLink</code> connects is immutable. However, if
 * the link is an instance of a particular type, its associated properties are mutable.
 * </p>
 *  
 * <p>
 * Implementations of this interfaces must provide a the <code>notifyTargetHandleUpdate</code> to allow
 * hypergraph to notify them when a target atom was loaded/unloaded from the database. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public interface HGLink
{
    /**
     * <p>Return the number of targets of this link. This number may be >= 0.</p>
     */
    int getArity();
    
    /**
     * <p>Return the ith target.</p>
     * 
     * @param i The index of the desired target. The range of this parameters must be
     * <code>[0...getArity() - 1]</code>.
     */
    HGHandle getTargetAt(int i);
    
    /**
     * <p>Notify the <code>HGLink</code> that one of its target atoms should be refered to
     * by a different <code>HGHandle</code> instance. Generally, implementation should update their reference
     * to this target with the passed in <em>live</em> handle.</p> 
     * 
     * <p>
     * <strong>IMPORTANT NOTE:</strong> This method should never be called by application
     * code. It is strictly reserved to the HyperGraph implementation which guarantuees that
     * the new handle will always refer to the same atom. The method should essentially
     * perform a <code>setTargetAt</code> operation, but a more elaborate name was chosen
     * to reflect the intended usage.
     * </p>
     * 
     * @param i The index of the target that was loaded.
     * @param handle The new <em>live</em> handle of the target atom.
     */
    void notifyTargetHandleUpdate(int i, HGHandle handle);
    
    /**
     * <p>
     * Notify the <code>HGLink</code> that one of its targets must be removed. This
     * method is invoked by the system when the target at position <code>i</code> refers
     * to an atom that is being deleted from the database. Implementation are required 
     * to remove the target at that position from their implementation data structure.
     * It remains the system's responsibility to reflect that change in permanent 
     * data storage.
     * </p>
     * 
     * <p>
     * An implementation may throw an <code>IllegalArgumentException</code> if the target
     * cannot be removed from the link because it would somehow break the semantics of the
     * application or lead in otherwise inconsistent state. Throwing such an exception would
     * indicate a fatal error and a very likely bug in the application.
     * </p>
     * 
     * @param i The 0-based position of the target to be removed from this link.
     * 
     * @throws IllegalArgumentException if the target cannot be removed from the link. 
     * 
     */
    void notifyTargetRemoved(int i);
}