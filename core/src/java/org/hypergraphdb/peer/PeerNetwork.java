package org.hypergraphdb.peer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.hypergraphdb.query.HGAtomPredicate;

/**
 * <p>
 * A representation of the peer network. Contains info about other peers and implements
 * the actual connection and communication with the network.
 * </p>
 * 
 * @author ciprian.costa
 */
public interface PeerNetwork
{
	boolean configure(Map<String, Object> config);
	void stop();

	void join(ExecutorService executorService);

	//get/set atom interests for known peers
	void setAtomInterests(Object peer, HGAtomPredicate interest);
	HGAtomPredicate getAtomInterests(Object peer);
	
	Object getPeerId(Object peer);
	
	void waitForRemotePipe();

	List<RemotePeer> getConnectedPeers();
	RemotePeer getConnectedPeer(String peerName);
	
	void addPeerPresenceListener(PeerPresenceListener listener);
	void removePeerPresenceListener(PeerPresenceListener listener);
}
