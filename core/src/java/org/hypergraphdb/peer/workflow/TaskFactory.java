package org.hypergraphdb.peer.workflow;

import org.hypergraphdb.peer.HyperGraphPeer;

public interface TaskFactory
{
	TaskActivity<?> newTask(HyperGraphPeer thisPeer, Object msg);
}