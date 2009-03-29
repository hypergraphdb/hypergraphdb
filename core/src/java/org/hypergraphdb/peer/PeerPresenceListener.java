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