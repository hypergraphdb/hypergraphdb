/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.cact;


import static org.hypergraphdb.peer.Messages.CONTENT;

import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.algorithms.CopyGraphTraversal;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.algorithms.HyperTraversal;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

public class TransferGraph extends FSMActivity
{
    public static final String TYPENAME = "transfer-graph";
    
    private HGPeerIdentity target;
    private HGTraversal traversal;
    private Mapping<Pair<HGHandle, Object>, HGHandle> atomFinder = null;
    private boolean trace = true;
    
    public TransferGraph(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public TransferGraph(HyperGraphPeer thisPeer, 
                         HGTraversal traversal, 
                         HGPeerIdentity target)
    {
        super(thisPeer);
        this.traversal = traversal;
        this.target = target;
    }
    
    public TransferGraph(HyperGraphPeer thisPeer, 
                         HGTraversal traversal, 
                         HGPeerIdentity target,
                         Mapping<Pair<HGHandle, Object>, HGHandle> atomFinder)
    {
        this(thisPeer, traversal, target);
        this.atomFinder = atomFinder;
    }
    
    @Override
    public void initiate()
    { 
    	Json msg = createMessage(Performative.QueryRef, this);
        msg.set(CONTENT, traversal); 
        send(target, msg);
        if (trace)
        	getThisPeer().getGraph().getLogger().trace("Query graph transfer for : " + traversal);
    }

    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onQueryRef(Json msg) throws Throwable
    {
        traversal = Messages.content(msg); 
        CopyGraphTraversal copyTraversal = null;
        if (traversal instanceof CopyGraphTraversal)
            copyTraversal = (CopyGraphTraversal)traversal;
        else if (traversal instanceof HyperTraversal)
        {
            ((HyperTraversal)traversal).setHyperGraph(getThisPeer().getGraph());
            copyTraversal = (CopyGraphTraversal)((HyperTraversal)traversal).getFlatTraversal();
        }
        else
            throw new Exception("Expecting a CopyGraphTraversal or a HyperTraversal.");
        if (trace)
        	getThisPeer().getGraph().getLogger().trace("Recevied request for traversal : " + copyTraversal);
        ((DefaultALGenerator)copyTraversal.getAdjListGenerator()).setGraph(getThisPeer().getGraph());
        Json reply = getReply(msg, Performative.InformRef);
        Object subgraph = SubgraphManager.getTransferGraphRepresentation(getThisPeer().getGraph(), traversal);
        reply.set(CONTENT, subgraph);
        send(getSender(msg), reply);
        if (trace)
        	getThisPeer().getGraph().getLogger().trace("Sent response to traversal : " + copyTraversal);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onInformRef(Json msg) throws ClassNotFoundException
    {
    	if (trace)
    		getThisPeer().getGraph().getLogger().trace("Received response for traversal : " + traversal);
        SubgraphManager.writeTransferedGraph(msg.at(CONTENT), 
                                             getThisPeer().getGraph(),
                                             atomFinder);
        if (trace)
        	getThisPeer().getGraph().getLogger().trace("Successfully stored graph for : " + traversal);
        return WorkflowState.Completed;
    }
    
    /*
     * @see org.hypergraphdb.peer.workflow.Activity#getType()
     */
    public String getType()
    {
        return TYPENAME;
    }
}