package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;

public class GetAtomType extends FSMActivity
{
    public static final String TYPENAME = "get-atom-type";
    
    private HGHandle handle;
    private HGHandle type;    
    private HGPeerIdentity target;
       
    public GetAtomType(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public GetAtomType(HyperGraphPeer thisPeer, HGHandle atom, HGPeerIdentity target)
    {
        super(thisPeer);
        this.handle = atom;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
    	Json msg = createMessage(Performative.QueryRef, this);
        msg.set(CONTENT, handle); 
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onGetType(Json msg) throws Throwable
    {
        handle = Messages.fromJson(msg.at(CONTENT));
        Json reply = getReply(msg, Performative.InformRef);
        reply.set(CONTENT, getThisPeer().getGraph().getType(handle)); 
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onTypeReceived(Json msg)
    {
        this.type = Messages.fromJson(msg.at(CONTENT));
        return WorkflowStateConstant.Completed;
    }

    public HGPeerIdentity getTarget()
    {
        return target;
    }

    public void setTarget(HGPeerIdentity target)
    {
        this.target = target;
    }
    
    public HGHandle getAtomHandle()
    {
        return handle;
    }
    
    public HGHandle getTypeHandle()
    {
        return type;
    }
    
    public String getType()
    {
        return TYPENAME;
    }
}