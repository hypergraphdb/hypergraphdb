package org.hypergraphdb.peer.workflow;

import org.hypergraphdb.peer.HyperGraphPeer;

public class RequestResponseTask extends TaskActivity<RequestResponseTask.State>
{
    enum State { Started, Done }
    
    public RequestResponseTask(HyperGraphPeer thisPeer)
    {
        super(thisPeer, State.Started, State.Done);
    }
    
    @Override
    protected void startTask()
    {
    }

    public static class Factory implements TaskFactory
    {
        public RequestResponseTask newTask(HyperGraphPeer thisPeer, Object msg)
        {
            return new RequestResponseTask(thisPeer);
        }
    }
}
