package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.createMessage;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.UUID;

import org.hypergraphdb.algorithms.HGTraversal;
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

public class TransferGraph extends FSMActivity
{
    public static final String TYPENAME = "transfer-graph";
    
    private HGPeerIdentity target;
    private HGTraversal traversal;
    
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
    
    @Override
    public void initiate()
    {
        Message msg = createMessage(Performative.QueryRef, this);
        combine(msg, struct(CONTENT, traversal)); 
        send(target, msg);        
    }

    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onQueryRef(Message msg) throws Throwable
    {
        traversal = getPart(msg, "CONTENT"); 
        // If we got here, all went well
        Message reply = getReply(msg, Performative.InformRef);
        Object subgraph = SubgraphManager.getTransferGraphRepresentation(getThisPeer().getGraph(), traversal);
        combine(reply, struct(CONTENT, subgraph));
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onInformRef(Message msg) throws ClassNotFoundException
    {
        SubgraphManager.writeTransferedGraph(getPart(msg, CONTENT), getThisPeer().getGraph());
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
