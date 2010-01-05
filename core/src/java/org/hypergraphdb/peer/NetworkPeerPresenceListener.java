/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

/**
 * <p>
 * A listener that tracks low-level network peer activity - to be invoked
 * when a peer joins/leaves the network before its HyperGraphDB identity
 * has be established/validates/authenticated.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface NetworkPeerPresenceListener
{
    void peerJoined(Object networkTarget);
    void peerLeft(Object networkTarget);
}
