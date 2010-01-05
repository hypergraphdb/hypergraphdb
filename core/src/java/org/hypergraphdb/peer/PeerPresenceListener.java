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
 * A listener that tracks when peers, whose HyperGraphDB identity has 
 * been established, join/leave the network.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface PeerPresenceListener
{
    void peerJoined(HGPeerIdentity peer);
    void peerLeft(HGPeerIdentity peer);
}
