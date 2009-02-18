package org.hypergraphdb.peer.workflow;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.HGDBOntology.*;
import static org.hypergraphdb.peer.protocol.Performative.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.HGDBOntology;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.protocol.Performative;

public class AffirmIdentityTask extends TaskActivity<AffirmIdentityTask.State>
{
    protected enum State {Started, Done}
    
    Object target = null;
    Object msg = null;
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
     
    public AffirmIdentityTask(HyperGraphPeer thisPeer)
    {
        this(thisPeer, null);
    }
    
    public AffirmIdentityTask(HyperGraphPeer thisPeer, Object msg)
    {
        super(thisPeer, State.Started, State.Done);
        this.msg = msg;        
    }
    
    public AffirmIdentityTask(HyperGraphPeer thisPeer, Object msg, Object target)
    {
        super(thisPeer, State.Started, State.Done);
        this.msg = msg;        
        this.target = target;
    }
    
    @Override
    protected void startTask()
    {
        if (msg == null)
        {
            Object inform = combine(createMessage(Inform, 
                                                  AFFIRM_IDENTITY, 
                                                  getTaskId()),
                                                  
                                    struct(CONTENT, 
                                           makeIdentityStruct(getThisPeer().getIdentity())));
            if (target == null)
                getPeerInterface().broadcast(inform);
            else
                getPeerInterface().send(target, inform);
        }
        else
            handleMessage(msg);
        // We don't need to maintain any special state in the task so we can safely treat
        // each incoming message as a new task. This is probably a common pattern: "stateless        
        // tasks"
        setState(State.Done);        
    }

    public void handleMessage(Object msg)
    {
        try
        {            
            HGPeerIdentity thisId = getThisPeer().getIdentity();
            Object x = getPart(msg, PERFORMATIVE);
            Performative perf = Performative.valueOf(x.toString());
            HGPeerIdentity id = parseIdentity(getStruct(msg, CONTENT));
            Object reply = getReply(msg);            
            if (perf == Inform)
            {
                if (id.getId().equals(thisId.getId()))
                    combine(reply, struct(PERFORMATIVE, Disconfirm));
                else
                {
                    combine(reply, combine(struct(PERFORMATIVE, Confirm),
                                           struct(CONTENT, 
                                                  makeIdentityStruct(getThisPeer().getIdentity()))));
                    getThisPeer().bindIdentityToNetworkTarget(id, getPart(msg, HGDBOntology.REPLY_TO));
                }
                sendReply(msg, reply);
            }
            else if (perf == Confirm)
            {
                getThisPeer().bindIdentityToNetworkTarget(id, getPart(msg, HGDBOntology.REPLY_TO));
            }
            else if (perf == Disconfirm)
            {
                // TODO
                // Change identity?
                // Spawn a new conversation to negotiate with other(s) peer(s) with same
                // identity?
                
                // NOTE: what happen now because HyperGraphPeer.getIdentity() changes the ID
                // right away in case of hostname/ipaddress/graphLocation mismatch.
            }            
        }
        catch (Throwable ex)
        {
            ex.printStackTrace(System.err);
        }        
    }
    
    public static class Factory implements TaskFactory
    {
        public TaskActivity<?> newTask(HyperGraphPeer thisPeer, Object msg)
        { return new AffirmIdentityTask(thisPeer, msg); }
    }    
}