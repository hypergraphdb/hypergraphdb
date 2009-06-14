package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;
import java.util.UUID;
import org.hypergraphdb.HGHandle;
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
    private HGHandle atom;
    
    public DefineAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public DefineAtom(HyperGraphPeer thisPeer, HGHandle atom, HGPeerIdentity target)
    {
        super(thisPeer);
        this.atom = atom;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
        Message msg = createMessage(Performative.Request, this);
        combine(msg, 
                struct(CONTENT, 
                       SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), atom))); 
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