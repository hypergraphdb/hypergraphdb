/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;


import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Performative.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import mjson.Json;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;

public class AffirmIdentity extends FSMActivity
{
    public static final String TYPE_NAME = "affirm-identity";
    
    Object target = null;
    AtomicInteger count = null;
    
    Json makeIdentityStruct(HGPeerIdentity identity)
    {
        return Json.object("uuid", identity.getId(),
                      "hostname", identity.getHostname(),
                      "ipaddress", identity.getIpAddress(),
                      "graph-location", identity.getGraphLocation() /*,
                      "name", identity.getName() */);
    }
    
    HGPeerIdentity parseIdentity(Json j)
    {
        HGPeerIdentity I = new HGPeerIdentity();
        I.setId((HGPersistentHandle)Messages.fromJson(j.at("uuid")));
        I.setHostname(j.at("hostname").asString());
        I.setIpAddress(j.at("ipaddress").asString());
        I.setGraphLocation(j.at("graph-location").asString());
        //I.setName(S.get("name").toString());
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
    	Json inform = createMessage(Inform, this)         
                                .set(Messages.CONTENT, 
                                     makeIdentityStruct(getThisPeer().getIdentity()));
        if (target == null)
            getPeerInterface().broadcast(inform);
        else
            getPeerInterface().send(target, inform);
    }

    @FromState("Started")
    @OnMessage(performative="Inform")
    @PossibleOutcome("Completed")
    public WorkflowState onInform(Json msg)
    {
        HGPeerIdentity thisId = getThisPeer().getIdentity();
        HGPeerIdentity id = parseIdentity(msg.at(Messages.CONTENT));
        Json reply = getReply(msg);        
        if (id.getId().equals(thisId.getId()))
            reply.set(Messages.PERFORMATIVE, Disconfirm);
        else
        {
            reply.set(Messages.PERFORMATIVE, Confirm)
                 .set(Messages.CONTENT, makeIdentityStruct(getThisPeer().getIdentity()));
            getThisPeer().bindIdentityToNetworkTarget(id, Messages.fromJson(msg.at(Messages.REPLY_TO)));
        }
        getPeerInterface().send(getSender(msg), reply);
        return WorkflowState.Completed;
    }

    @FromState("Started")
    @OnMessage(performative="Confirm")
    @PossibleOutcome("Completed")    
    public WorkflowState onConfirm(Json msg)
    {
        HGPeerIdentity id = parseIdentity(msg.at(Messages.CONTENT));
        getThisPeer().bindIdentityToNetworkTarget(id, getSender(msg));
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Disconfirm")
    @PossibleOutcome("Failed")    
    public WorkflowState onDisconfirm(Json msg)
    {
        return WorkflowState.Failed;
    }
}
