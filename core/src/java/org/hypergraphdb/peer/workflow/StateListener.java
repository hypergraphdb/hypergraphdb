package org.hypergraphdb.peer.workflow;

public interface StateListener
{
    void stateChanged(WorkflowState state);
}