/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * A <code>HGAtomType</code> is an object capable of translating run-time atom instances to/from
 * data in <code>HGStore</code> as well as providing minimal semantic information about
 * the entities being constructed in the form of a predicate, called <code>subsumes</code> that 
 * determines whether one entity is a specialization of another.
 * </p>
 *
 * <p><code>HGAtomType</code>s deal with hypergraph atom values, their layout and management in
 * the <code>HGStore</code>. An atom type can be seen as a  constructor that "knows" how
 * to build an appropriately typed, in-memory instance of an atom. It is capable of constructing
 * both node and link instances. It is also capable of recording an atom value in the <code>HGStore</code>
 * or removing it from there.</p> 
 * 
 * <p>
 * The motivation behind the <code>subsumes</code> predicate is the notion of sub-typing. However, it
 * has been lifted into a general semantic partial order relation between entities. If A <code>subsumes</code>
 * B and B <code>subsumes</code> A then they are equivalent. For a more thourough discussion of the
 * <code>subsumes</code> predicate and how it fits into the overall framework of HyperGraphDB's type
 * system and general data management, please consult the HyperGraphDB manual. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGAtomType extends HGGraphHolder
{   
    /**
     * <p>Construct a new run-time instance of a hypergraph atom. A plain node must
     * be constructed whenever the <code>targetSet</code> parameter is null or of 
     * length 0. Otherwise, a <code>HGLink</code> instance must be constructed.</p>
     * 
     * <p>
     * It is not required that all atom types be able to construct both plain (node)
     * atoms and <code>HGLink</code>s. It is up to an <code>HGAtomType</code> implementation
     * to support either or both. When a <code>HGLink</code> counterpart is not available
     * for a particular run-time type, an implementation may choose to create an instance
     * of the default link value holder implementation <code>HGValuedLink</code>, provided
     * by HyperGraph.
     * </p>
     * 
     * @param handle The <code>HGPersistentHandle</code> of the atom value.
     * @param targetSet When the atom is a link, this parameter holds the target set of
     * the link. When the atom is a node, the parameter is an array of 0 length.
     * @param incidenceSet A lazy reference to the set of links pointing to this atom. This is
     * <code>null</code> if we are constructing an internal/nested value of some complex type.
     * @return The run-time atom instance. The return value should never be <code>null</code>.
     * In case the <code>handle</code> points to an invalid instance (inexisting or with
     * a erronous layout), the method should throw a <code>HGException</code>.
     */
    Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet);
    
    /**
     * <p>Store a run-time instance of a hypergraph atom into the hypergraph <code>HGStore</code>
     * as a new atom.</p>
     * 
     * @param instance The atom instance.
     * @return The persistent handle of the stored value. 
     */
    HGPersistentHandle store(Object instance);
    
    
    /**
     * <p>Release a hypergraph value instance from the persistent store.</p>
     * 
     * <p>
     * This method should be called when a <code>HGPersistentHandle</code> 
     * returned from the <code>store</code> is no longer in use. 
     * </p>
     * 
     * @param handle The persistent handle of the value to release.
     */
    void release(HGPersistentHandle handle);
    
    /**
     * <p>A generic semantic predicate that returns <code>true</code> if the first argument 
     * is more general than the second. Atom types must implement this notion of <em>specialization</em>
     * whenever meaningful in the context of the entities being constructed.</p>
     * 
     * <p>The notion of subsumption can be seen as <em>partial equivalence</em>, or equivalence
     * in one direction only.</p>
     * 
     * <p>
     * As a relation, subsumtion is transitive and reflexive. The latter implies that, at a minimum,
     * the <code>subsumes</code> method must return <code>true</code> if 
     * <code>general.equals(specific)</code>. 
     * </p>
     * 
     * @param general The object which might be more general. Cannot be <code>null</code>.
     * @param specific The object which might be more specific. Cannot be <code>null</code>.
     * @return <code>true<code> if <code>specific</code> can be used whenever <code>general</code>
     * is required and <code>false</code> otherwise.
     */
    boolean subsumes(Object general, Object specific);
}