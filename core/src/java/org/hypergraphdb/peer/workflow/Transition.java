package org.hypergraphdb.peer.workflow;

public interface Transition
{
    public WorkflowStateConstant apply(Activity activity, Object...args);
}