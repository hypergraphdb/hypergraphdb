package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.createMessage;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.list;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
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

public class AddAtom extends FSMActivity
{
    public static final String TYPENAME = "add-atom";
    
    private HGHandle typeHandle;
    private Object atom;
    private HGHandle atomHandle;
    private HGPeerIdentity target;
    
    private HGHandle [] getAtomTargets()
    {
        if (! (atom instanceof HGLink))
          return HGUtils.EMPTY_HANDLE_ARRAY;
        else
            return HGUtils.toHandleArray((HGLink)atom);
    }
    
    public AddAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public AddAtom(HyperGraphPeer thisPeer, Object atom, HGPeerIdentity target)
    {
        this(thisPeer, atom, null, target);
    }
    
    public AddAtom(HyperGraphPeer thisPeer, Object atom, HGHandle typeHandle, HGPeerIdentity target)
    {
        super(thisPeer);
        this.atom = atom;
        this.typeHandle = typeHandle;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
        HyperGraph graph = getThisPeer().getGraph();
        Message msg = createMessage(Performative.Request, this);
        if (typeHandle == null)
            typeHandle = getThisPeer().getGraph().getTypeSystem().getTypeHandle(atom);
        HGAtomType  type = getThisPeer().getGraph().get(typeHandle);
        StorageGraph sgraph = new RAMStorageGraph();
        getThisPeer().getGraph().getStore().attachOverlayGraph(sgraph);
        try
        {
            sgraph.getRoots().add(type.store(atom));
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
            combine(msg, 
                    struct(CONTENT,             
                           struct("storage-graph", object(sgraph),
                                  "type-handle", typeHandle, 
                                  "type-classes", types,
                                  "targets", list((Object[])getAtomTargets()))));
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
    public WorkflowStateConstant onAddAtom(Message msg) throws Throwable
    {
        HyperGraph graph = getThisPeer().getGraph();
        Object A = getPart(msg, CONTENT);
        final RAMStorageGraph subgraph = getPart(A, "storage-graph");        
        final Map<String, String>  typeClasses = getPart(A, "type-classes");
        final HGHandle typeHandle = getPart(A, "type-handle");
        final List<Object> targets = getPart(A, "targets");
        Map<HGHandle, HGHandle> localTypes = SubgraphManager.getLocalTypes(graph, typeClasses); 
        subgraph.translateHandles(localTypes);
        HGHandle ltype = localTypes.get(typeHandle);
        if (ltype == null)
            ltype = typeHandle;        
        graph.getStore().attachOverlayGraph(subgraph);
        try
        {    
            HGHandle [] targetSet = targets.toArray(HGUtils.EMPTY_HANDLE_ARRAY);
            HGAtomType type = graph.get(ltype);                    
            atom = type.make(subgraph.getRoots().iterator().next(), 
                             new ReadyRef<HGHandle[]>(targetSet), 
                             null);                       
        }
        finally
        {
            graph.getStore().detachOverlayGraph();
        }         
        this.atomHandle = graph.add(atom, ltype);
        reply(msg, Performative.Agree, struct("atom-handle", atomHandle));
        return WorkflowStateConstant.Completed;
    }
     
    @FromState("Started")
    @OnMessage(performative="Agree")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onAtomAdded(Message msg) throws Throwable
    {
        this.atomHandle = getPart(msg, CONTENT, "atom-handle");
        return WorkflowStateConstant.Completed;
    }
    
    public HGHandle getAtomHandle()
    {
        return this.atomHandle;
    }
    
    public String getType()
    {
        return TYPENAME;
    }
}