package org.hypergraphdb.peer.workflow;

import java.util.UUID;
import org.hypergraphdb.peer.HyperGraphPeer;

public interface ActivityFactory
{
	Activity make(HyperGraphPeer thisPeer, UUID id, Object msg);
}