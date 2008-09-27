package org.hypergraphdb.peer.workflow;

import org.hypergraphdb.peer.PeerInterface;

public interface TaskFactory
{
	TaskActivity<?> newTask(PeerInterface peerInterface, Object msg);
}
