/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.cact;



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
import static org.hypergraphdb.peer.Messages.*;

public class GetClassForType extends FSMActivity
{
    public static final String TYPENAME = "get-class-for-type";
    
    private HGPeerIdentity target;
    private HGHandle typeHandle;
    private String className;
    
    public GetClassForType(HyperGraphPeer thisPeer, HGHandle typeHandle, Object target)
    {
        super(thisPeer);        
        if (target instanceof HGPeerIdentity)
            this.target = (HGPeerIdentity)target;
        else
            this.target = thisPeer.getIdentity(target);
        this.typeHandle = typeHandle;
    }

    public GetClassForType(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    @Override
    public void initiate()
    {
    	Json msg = createMessage(Performative.QueryRef, this);
        msg.set(Messages.CONTENT, typeHandle);
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onQuery(Json msg)
    {
        typeHandle = Messages.fromJson(msg.at("content"));
        Class<?> clazz = getThisPeer().getGraph().getTypeSystem().getClassForType(typeHandle);
        if (clazz != null)
            send(getSender(msg), 
                 getReply(msg, Performative.Inform, clazz.getName()));
        else
            send(getSender(msg),
                 getReply(msg, Performative.Refuse));
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Inform")
    @PossibleOutcome("Completed")
    public WorkflowStateConstant onInform(Json msg)
    {
        className = msg.at(CONTENT).asString(); 
        return WorkflowState.Completed;
    }    
    
    @FromState("Started")
    @OnMessage(performative="Refuse")
    @PossibleOutcome("Failed")
    public WorkflowStateConstant onRefuse(Json msg)
    { 
        return WorkflowState.Failed;
    }
    
    public String getClassName()
    {
        return className;
    }
    
    public String getType()
    {
        return TYPENAME;
    }
}