package org.hypergraphdb.peer;

public interface PeerPresenceListener
{
    void peerJoined(Object networkTarget);
    void peerLeft(Object networkTarget);
}