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