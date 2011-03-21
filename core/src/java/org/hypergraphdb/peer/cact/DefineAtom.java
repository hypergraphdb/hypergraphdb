/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;

import java.util.UUID;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGUtils;

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
    
    private HGHandle [] getAtomTargets()
    {
        if (! (atom instanceof HGLink))
          return HGUtils.EMPTY_HANDLE_ARRAY;
        else
            return HGUtils.toHandleArray((HGLink)atom);
    }
    
    public DefineAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public DefineAtom(HyperGraphPeer thisPeer, HGHandle atom, HGPeerIdentity target)
    {
        super(thisPeer);
        this.atomHandle = atom;
        this.target = target;
    }

    public DefineAtom(HyperGraphPeer thisPeer, Object atom, HGHandle typeHandle, HGPeerIdentity target)
    {
        super(thisPeer);
        this.atom = atom;
        this.typeHandle = typeHandle;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
        Message msg = createMessage(Performative.Request, this);
        if (atomHandle != null)
            combine(msg, 
                    struct(CONTENT, 
                           SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), atomHandle)));
        else
        {
            if (typeHandle == null)
                typeHandle = getThisPeer().getGraph().getTypeSystem().getTypeHandle(atom);
            HGAtomType  type = getThisPeer().getGraph().get(typeHandle);
            StorageGraph sgraph = new RAMStorageGraph();
            getThisPeer().getGraph().getStore().attachOverlayGraph(sgraph);
            try
            {
                sgraph.getRoots().add(type.store(atom));
                combine(msg, 
                        struct(CONTENT,             
                               struct("storage-graph", object(sgraph),
                                      "type-handle", typeHandle, 
                                      "type-classes", struct()),
                                      "targets", list((Object[])getAtomTargets())));
                send(target, msg);
            }
            finally
            {
                getThisPeer().getGraph().getStore().detachOverlayGraph();
            }
        }        
        send(target, msg);        
    }

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRequestDefine(Message msg) throws Throwable
    {
        SubgraphManager.writeTransferedGraph(getPart(msg, CONTENT), getThisPeer().getGraph());
        // If we got here, all went well
        Message reply = getReply(msg, Performative.Agree);
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onAgree(Message msg)
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
