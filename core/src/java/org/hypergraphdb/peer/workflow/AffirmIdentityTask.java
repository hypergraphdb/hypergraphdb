package org.hypergraphdb.peer.workflow;

import org.hypergraphdb.peer.PeerInterface;

public class AffirmIdentityTask extends TaskActivity<AffirmIdentityTask.State>
{
    protected enum State {Started, Done}
    
    Object msg;
    
    public AffirmIdentityTask(PeerInterface peerInterface)
    {
        this(peerInterface, null);
    }
    
    public AffirmIdentityTask(PeerInterface peerInterface, Object msg)
    {
        super(peerInterface, State.Started, State.Done);
        this.msg = msg;        
    }
    
    @Override
    protected void startTask()
    {
    }

    public void stateChanged(Object newState, AbstractActivity<?> activity)
    {
    }

    public static class Factory implements TaskFactory
    {
        public TaskActivity<?> newTask(PeerInterface peerInterface, Object msg)
        { return new AffirmIdentityTask(peerInterface, msg); }
    }    
}