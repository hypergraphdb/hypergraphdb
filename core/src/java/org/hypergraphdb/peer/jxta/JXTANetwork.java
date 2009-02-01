package org.hypergraphdb.peer.jxta;

import java.util.Set;

import net.jxta.document.Advertisement;
import net.jxta.peergroup.PeerGroup;
import net.jxta.pipe.PipeID;

import org.hypergraphdb.peer.PeerNetwork;

public interface JXTANetwork extends PeerNetwork
{
	PeerGroup getPeerGroup();
	
	void publishAdv(Advertisement adv);
	void addOwnPipe(PipeID pipeId);
	Set<Advertisement> getAdvertisements();
	Advertisement getPipeAdv();

	
}
