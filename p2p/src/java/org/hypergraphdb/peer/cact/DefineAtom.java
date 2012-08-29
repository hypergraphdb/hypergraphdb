/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.cact;


import static org.hypergraphdb.peer.Messages.*;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;

/**
 * <p>
 * Used to store an atom at a target peer. The initiating peer sends a Performative.Request
 * to the target peer with the full storage graph of the atom to define. 
 * The target peer writes the atom locally and replies with Performative.Agree.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class DefineAtom extends FSMActivity
{
    public static final String TYPENAME = "define-atom";
    
    private HGPeerIdentity target;
    private HGHandle atomHandle;
    private HGHandle typeHandle;
    private Object atom;
    
    public DefineAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public DefineAtom(HyperGraphPeer thisPeer, HGHandle atomHandle, HGPeerIdentity target)
    {
        super(thisPeer);
        this.atomHandle = atomHandle;
        this.target = target;
    }

    public DefineAtom(HyperGraphPeer thisPeer, 
                      HGHandle atomHandle, 
                      Object atom, 
                      HGHandle typeHandle, 
                      HGPeerIdentity target)
    {
        super(thisPeer);
        this.atomHandle = atomHandle;
        this.atom = atom;
        this.typeHandle = typeHandle;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
        HyperGraph graph = getThisPeer().getGraph();
        Json msg = createMessage(Performative.Request, this);
        if (graph.get(atomHandle) != null)
        {
            msg.set(CONTENT, SubgraphManager.getTransferAtomRepresentation(graph, atomHandle));
            send(target, msg);            
        }
        else
        {
            msg.set(CONTENT, SubgraphManager.getTransferObjectRepresentation( graph, atomHandle, atom, typeHandle));
            send(target, msg);            
        }        
    }

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRequestDefine(Json msg) throws Throwable
    {
        SubgraphManager.writeTransferedGraph(msg.at(CONTENT), getThisPeer().getGraph());
        // If we got here, all went well
        Json reply = getReply(msg, Performative.Agree);
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onAgree(Json msg)
    {
        return WorkflowStateConstant.Completed;
    }
    
    /*
     * @see org.hypergraphdb.peer.workflow.Activity#getType()
     */
    public String getType()
    {
        return TYPENAME;
    }
}