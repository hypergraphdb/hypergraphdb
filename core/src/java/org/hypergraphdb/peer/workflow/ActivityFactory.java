package org.hypergraphdb.peer.workflow;

import java.util.UUID;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;

public interface ActivityFactory
{
	Activity make(HyperGraphPeer thisPeer, UUID id, Message msg);
}