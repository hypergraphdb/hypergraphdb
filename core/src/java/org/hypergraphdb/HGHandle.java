/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

/**
 * <p>
 * A <code>HGHandle</code> represents a reference to a hypergraph atom. Hypergraph references
 * are completely managed by HyperGraphDB and should be treated by applications as abstract
 * reference types.
 * </p>
 * 
 * <p>
 * A hypergraph handle holds the system-level identity of an atom. HyperGraphDB will return
 * concrete implementations of this interface dependending on configuration, context of usage
 * and similar considerations. In other words, a handle is essentially a reference token. One can think of a handle as a 
 * memory location, a GUID (Globally Unique Identifier) or a URI. 
 * </p>
 * 
 * <p>
 * Generally, handles are only valid during the run-time of a system. To obtain a permanent handle, 
 * one which transcends system startup and shutdown, the <code>HyperGraph.getPersistentHandle</code>
 * method should be used.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGHandle
{
}