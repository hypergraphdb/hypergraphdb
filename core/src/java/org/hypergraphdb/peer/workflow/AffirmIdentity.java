package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.protocol.Performative.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Messages;

public class AffirmIdentity extends FSMActivity
{
    public static final String TYPE_NAME = "affirm-identity";
    
    Object target = null;
    AtomicInteger count = null;
    
    Object makeIdentityStruct(HGPeerIdentity identity)
    {
        return struct("uuid", identity.getId(),
                      "hostname", identity.getHostname(),
                      "ipaddress", identity.getIpAddress(),
                      "graph-location", identity.getGraphLocation(),
                      "name", identity.getName());
    }
    
    HGPeerIdentity parseIdentity(Map<String, Object> S)
    {
        HGPeerIdentity I = new HGPeerIdentity();
        I.setId((HGPersistentHandle)getPart(S, "uuid"));
        I.setHostname(S.get("hostname").toString());
        I.setIpAddress(S.get("ipaddress").toString());
        I.setGraphLocation(S.get("graph-location").toString());
        I.setName(S.get("name").toString());
        return I;
    }
     
    public AffirmIdentity(HyperGraphPeer thisPeer)
    {
        this(thisPeer, null);
    }
    
    public AffirmIdentity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);        
    }
    
    public AffirmIdentity(HyperGraphPeer thisPeer, Object target)
    {
        super(thisPeer, UUID.randomUUID());        
        this.target = target;
    }
    
    public String getType()
    {
        return TYPE_NAME;        
    }
    
    public void initiate()
    {
        Message inform = combine(createMessage(Inform,
                                              this),                                              
                                struct(Messages.CONTENT, 
                                       makeIdentityStruct(getThisPeer().getIdentity())));
        if (target == null)
            getPeerInterface().broadcast(inform);
        else
            getPeerInterface().send(target, inform);
    }

    @FromState("Started")
    @OnMessage(performative="Inform")
    @PossibleOutcome("Completed")
    public WorkflowState onInform(Message msg)
    {
        HGPeerIdentity thisId = getThisPeer().getIdentity();
        HGPeerIdentity id = parseIdentity(getStruct(msg, Messages.CONTENT));
        Message reply = getReply(msg);        
        if (id.getId().equals(thisId.getId()))
            combine(reply, struct(Messages.PERFORMATIVE, Disconfirm));
        else
        {
            combine(reply, combine(struct(Messages.PERFORMATIVE, Confirm),
                                   struct(Messages.CONTENT, 
                                          makeIdentityStruct(getThisPeer().getIdentity()))));
            getThisPeer().bindIdentityToNetworkTarget(id, getPart(msg, Messages.REPLY_TO));
        }
        getPeerInterface().send(getSender(msg), reply);
        return WorkflowState.Completed;
    }

    @FromState("Started")
    @OnMessage(performative="Confirm")
    @PossibleOutcome("Completed")    
    public WorkflowState onConfirm(Message msg)
    {
        HGPeerIdentity id = parseIdentity(getStruct(msg, Messages.CONTENT));
        getThisPeer().bindIdentityToNetworkTarget(id, getPart(msg, Messages.REPLY_TO));
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Disconfirm")
    @PossibleOutcome("Failed")    
    public WorkflowState onDisconfirm(Message msg)
    {
        return WorkflowState.Failed;
    }
}