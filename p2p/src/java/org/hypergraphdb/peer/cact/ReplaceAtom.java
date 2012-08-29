package org.hypergraphdb.peer.cact;


import static org.hypergraphdb.peer.Messages.CONTENT;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Pair;

public class ReplaceAtom extends FSMActivity
{
    public static final String TYPENAME = "replace-atom";
    
    private HGHandle atomHandle;
    private HGHandle typeHandle;
    private Object value;
    private HGPeerIdentity target;
    private Boolean replaced;
    
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
        HyperGraph graph = getThisPeer().getGraph();
        Json msg = createMessage(Performative.Request, this);
        HGAtomType  type = getThisPeer().getGraph().get(typeHandle);
        StorageGraph sgraph = new RAMStorageGraph();
        getThisPeer().getGraph().getStore().attachOverlayGraph(sgraph);
        try
        {
            sgraph.getRoots().add(type.store(value));
            Map<String, String> types = new HashMap<String, String>();
            String clname = graph.getTypeSystem().getClassNameForType(typeHandle);
            if (clname != null)
                types.put(typeHandle.getPersistent().toString(), clname);
            for (Pair<HGPersistentHandle, Object> p : sgraph)
            {
                if (p == null)
                    continue;
                if (p.getSecond() instanceof HGPersistentHandle[])
                {
                    for (HGPersistentHandle h : (HGPersistentHandle[])p.getSecond())
                        if ( (clname = graph.getTypeSystem().getClassNameForType(h)) != null )
                            types.put(h.toString(), clname);
                }
                clname = graph.getTypeSystem().getClassNameForType(p.getFirst());
                if (clname != null)
                    types.put(p.getFirst().getPersistent().toString(), clname);
            }                 
            msg.set(CONTENT,             
                           Json.object("atom-handle", atomHandle,
                                  "storage-graph", SubgraphManager.encodeSubgraph(sgraph),
                                  "type-handle", typeHandle, 
                                  "type-classes", types,
                                  "targets", getAtomTargets()));
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
    public WorkflowStateConstant onReplaceAtom(Json msg) throws Throwable
    {
        HyperGraph graph = getThisPeer().getGraph();
        Json A = msg.at(CONTENT);
        this.atomHandle = Messages.fromJson(A.at("atom-handle"));        
        final RAMStorageGraph subgraph = SubgraphManager.decodeSubgraph(A.at("storage-graph").asString());        
        final Map<String, String>  typeClasses = Messages.fromJson(A.at("type-classes"));
        this.typeHandle = Messages.fromJson(A.at("type-handle"));
        final HGHandle [] targets = Messages.fromJson(A.at("targets"));
        Map<HGHandle, HGHandle> localTypes = SubgraphManager.getLocalTypes(graph, typeClasses); 
        subgraph.translateHandles(localTypes);
        HGHandle ltype = localTypes.get(typeHandle);
        if (ltype == null)
            ltype = typeHandle;        
        graph.getStore().attachOverlayGraph(subgraph);
        try
        {    
            HGAtomType type = graph.get(ltype);                    
            value = type.make(subgraph.getRoots().iterator().next(), 
                             new ReadyRef<HGHandle[]>(targets), 
                             null);                       
        }
        finally
        {
            graph.getStore().detachOverlayGraph();
        }                 
        replaced = graph.replace(atomHandle, value, ltype);
        reply(msg, Performative.Agree, replaced);
        return WorkflowStateConstant.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onAtomReplaced(Json msg) throws Throwable
    {
        replaced = msg.at(CONTENT).asBoolean();
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