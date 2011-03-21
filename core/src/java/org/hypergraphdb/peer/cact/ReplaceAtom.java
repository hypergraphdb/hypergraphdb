package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;

import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.list;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGUtils;

public class ReplaceAtom extends FSMActivity
{
    public static final String TYPENAME = "replace-atom";
    
    private HGHandle atomHandle;
    private HGHandle typeHandle;
    private Object value;
    private HGPeerIdentity target;
    private boolean replaced;
    
    private HGHandle [] getAtomTargets()
    {
        if (! (value instanceof HGLink))
          return HGUtils.EMPTY_HANDLE_ARRAY;
        else
            return HGUtils.toHandleArray((HGLink)value);
    }
     
    public ReplaceAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public ReplaceAtom(HyperGraphPeer thisPeer, 
                       HGHandle atomHandle, 
                       Object value,
                       HGHandle typeHandle,
                       HGPeerIdentity target)
    {
        super(thisPeer);
        this.atomHandle = atomHandle;
        this.value = value;
        this.typeHandle = typeHandle;
        this.target = target;
    }

    public void initiate()
    {
        Message msg = createMessage(Performative.Request, this);
        HGAtomType type = getThisPeer().getGraph().get(typeHandle);
        StorageGraph sgraph = new RAMStorageGraph();
        getThisPeer().getGraph().getStore().attachOverlayGraph(sgraph);
        try
        {
            sgraph.getRoots().add(type.store(value));
            combine(msg, 
                    struct(CONTENT,             
                           struct("atom-handle", atomHandle,
                                  "storage-graph", object(sgraph),
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

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onReplaceAtom(Message msg) throws Throwable
    {
        HyperGraph graph = getThisPeer().getGraph();
        Object A = getPart(msg, CONTENT);
        atomHandle = getPart(A, "atom-handle");
        typeHandle = getPart(A, "type-handle");
        final RAMStorageGraph subgraph = getPart(A, "storage-graph");        
        final Map<String, String>  typeClasses = getPart(A, "type-classes");        
        final List<Object> targets = getPart(A, "targets");
        graph.getStore().attachOverlayGraph(subgraph);
        try
        {    
            HGHandle [] targetSet = targets.toArray(HGUtils.EMPTY_HANDLE_ARRAY);            
            HGAtomType type = graph.get(typeHandle);                    
            value = type.make(subgraph.getRoots().iterator().next(), 
                             new ReadyRef<HGHandle[]>(targetSet), 
                             null);                
        }
        finally
        {
            graph.getStore().detachOverlayGraph();
        }         
        replaced = graph.replace(atomHandle, value, typeHandle);
        reply(msg, Performative.Agree, replaced);
        return WorkflowStateConstant.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onAtomReplaced(Message msg) throws Throwable
    {
        replaced = getPart(msg, CONTENT);
        return WorkflowStateConstant.Completed;
    }
    
    public boolean getReplaced()
    {
        return replaced;
    }
    
    public HGHandle getAtomHandle()
    {
        return atomHandle;
    }

    public void setAtomHandle(HGHandle atomHandle)
    {
        this.atomHandle = atomHandle;
    }

    public Object getValue()
    {
        return value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    public HGPeerIdentity getTarget()
    {
        return target;
    }

    public void setTarget(HGPeerIdentity target)
    {
        this.target = target;
    }
    
    public String getType() { return TYPENAME; } 
}
