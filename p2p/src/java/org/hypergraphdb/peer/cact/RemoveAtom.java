package org.hypergraphdb.peer.cact;


import static org.hypergraphdb.peer.Messages.CONTENT;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
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

public class RemoveAtom extends FSMActivity
{
    public static final String TYPENAME = "remove-atom";
    
    private Set<HGHandle> handles;
    private HGPeerIdentity target;
    private Map<HGHandle, Boolean> removed;
    
    public RemoveAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public RemoveAtom(HyperGraphPeer thisPeer, HGHandle atomHandle, HGPeerIdentity target)
    {
        super(thisPeer);
        this.handles = new HashSet<HGHandle>();
        this.handles.add(atomHandle);
        this.target = target;
    }

    public RemoveAtom(HyperGraphPeer thisPeer, Collection<HGHandle> atomHandles, HGPeerIdentity target)
    {
        super(thisPeer);
        this.handles = new HashSet<HGHandle>();
        this.handles.addAll(atomHandles);
        this.target = target;
    }
    
    public void initiate()
    {
    	Json msg = createMessage(Performative.Request, this);
        msg.set(CONTENT, handles); 
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRemoveAtoms(Json msg) throws Throwable
    {
        handles = Messages.fromJson(msg.at(CONTENT));
        final HyperGraph graph = getThisPeer().getGraph();
        removed = new HashMap<HGHandle, Boolean>();
        final Json response = Json.object();
        graph.getTransactionManager().ensureTransaction(new Callable<Object>() {
        public Object call()
        {
            for (HGHandle h : handles)
            {
            	boolean b = getThisPeer().getGraph().remove(h);
                removed.put(h, b);
                response.set(h.getPersistent().toString(), b);
            }
            return null;
        }
        });
        reply(msg, Performative.Agree, response);
        return WorkflowState.Completed;                    
    }
 
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onAtomsRemoved(Json msg) throws Throwable
    {
        Json R = msg.at(CONTENT);
        removed = new HashMap<HGHandle, Boolean>();
        for (Map.Entry<String, Json> j : R.asJsonMap().entrySet())
        	removed.put(getThisPeer().getGraph().getHandleFactory().makeHandle(j.getKey()), 
        				j.getValue().asBoolean());
        return WorkflowState.Completed;
    }
    
    public Map<HGHandle, Boolean> getRemoved()
    {
        return removed;
    }
    
    public String getType() { return TYPENAME; }
}