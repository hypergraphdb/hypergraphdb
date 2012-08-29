package org.hypergraphdb.peer.cact;


import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.IncidenceSet;
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
import org.hypergraphdb.util.HGSortedSet;

public class GetIncidenceSet extends FSMActivity
{
    public static final String TYPENAME = "get-atom";
    
    private HGHandle handle;
    private IncidenceSet incidenceSet;    
    private HGPeerIdentity target;    
    
    public GetIncidenceSet(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public GetIncidenceSet(HyperGraphPeer thisPeer, HGHandle atom, HGPeerIdentity target)
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
    public WorkflowStateConstant onGetAtoms(Json msg) throws Throwable
    {
        handle = Messages.fromJson(msg.at(CONTENT));
        incidenceSet = getThisPeer().getGraph().getIncidenceSet(handle);
        Json reply = getReply(msg, Performative.InformRef);
        reply.set(CONTENT, incidenceSet); 
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onAtomsReceived(Json msg)
    {
        HGSortedSet<HGHandle> S = Messages.content(msg);
        this.incidenceSet = new IncidenceSet(handle, S);
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
    
    public String getType()
    {
        return TYPENAME;
    }

    public HGHandle getHandle()
    {
        return handle;
    }

    public IncidenceSet getIncidenceSet()
    {
        return incidenceSet;
    }
}