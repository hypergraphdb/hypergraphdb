package org.hypergraphdb.peer.workflow;

public interface ActivityStateListener
{
	void stateChanged(Object newState, AbstractActivity<?> activity);
}
