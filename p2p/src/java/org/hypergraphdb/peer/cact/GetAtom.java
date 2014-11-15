package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.ActivityListener;
import org.hypergraphdb.peer.workflow.ActivityResult;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.ChainResolver;
import org.hypergraphdb.util.HGAtomResolver;
import org.hypergraphdb.util.MapResolver;

public class GetAtom extends FSMActivity
{   
    public static final String TYPENAME = "get-atom";
    
    private Set<HGHandle> handles;
    private Map<HGHandle, Object> atomMap;    
    private HGPeerIdentity target;
    
    private void getRemoteTypes(final HGPeerIdentity from, 
                                final StorageGraph subgraph,
                                final Set<HGHandle> types,
                                final Map<HGHandle, HGHandle> typeMap)
    {
        GetAtom getTypeActivity = new GetAtom(getThisPeer(), types, from);
        ActivityListener continuation = new ActivityListener()
        {
            @SuppressWarnings("unchecked")
            public void activityFinished(ActivityResult result)
            {
                if (result.getActivity().getState().isCompleted())
                    readAtoms(subgraph, 
                              typeMap, 
                              (Map<HGHandle, HGAtomType>)(Map<?,?>)((GetAtom)result.getActivity()).atomMap);
                else
                    GetAtom.this.getState().assign(result.getActivity().getState().getConst());
            }            
        };
        getThisPeer().getActivityManager().initiateActivity(getTypeActivity, this, continuation);
    }
    
    private void readAtoms(StorageGraph sgraph, Map<HGHandle, HGHandle> typeMap, Map<HGHandle, HGAtomType> types)
    {
        atomMap = new HashMap<HGHandle, Object>();
        for (HGPersistentHandle handle : sgraph.getRoots())
            atomMap.put(handle, SubgraphManager.readAtom(
             handle, 
             getThisPeer().getGraph(),
             types == null ? new HGAtomResolver<HGAtomType>(getThisPeer().getGraph())
                           :
                             new ChainResolver<HGHandle, HGAtomType>(
                                     new MapResolver<HGHandle, HGAtomType>(types), 
                                     new HGAtomResolver<HGAtomType>(getThisPeer().getGraph())),
             sgraph));
    }
    
    public GetAtom(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public GetAtom(HyperGraphPeer thisPeer, HGHandle atom, HGPeerIdentity target)
    {
        super(thisPeer);
        this.handles = new HashSet<HGHandle>();
        this.handles.add(atom);
        this.target = target;
    }

    public GetAtom(HyperGraphPeer thisPeer, Set<HGHandle> atoms, HGPeerIdentity target)
    {
        super(thisPeer);
        this.handles = atoms;
        this.target = target;
    }
    
    @Override
    public void initiate()
    {
    	Json msg = createMessage(Performative.QueryRef, this);
        msg.set(CONTENT, handles); 
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onGetAtoms(Json msg) throws Throwable
    {
        handles = Messages.fromJson(msg.at(CONTENT));
        Json reply = getReply(msg, Performative.InformRef);
        reply.set(CONTENT, 
                  SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), handles)); 
        send(getSender(msg), reply);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("Completed")        
    public WorkflowStateConstant onAtomsReceived(Json msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        Json A = msg.at(CONTENT);
        final RAMStorageGraph subgraph = SubgraphManager.decodeSubgraph(A.at("storage-graph").asString());
        final Map<String, String>  typeClasses = Messages.fromJson(A.at("type-classes"));
        
        final Map<HGHandle, HGHandle> typeMap = 
            SubgraphManager.getLocalTypes(graph, typeClasses);
        
        // First let's check that we have all types locally, and if not we need to query for them.
        Set<HGHandle> missingTypes = new HashSet<HGHandle>();
        for (Iterator<Map.Entry<HGHandle, HGHandle>> I = typeMap.entrySet().iterator(); 
             I.hasNext(); )
        {
            Map.Entry<HGHandle, HGHandle> e = I.next();
            if (!e.getKey().equals(e.getValue()))
                continue;
            if (graph.get(e.getKey()) == null)
                missingTypes.add(e.getKey());
            I.remove();
        }
        
        subgraph.translateHandles(typeMap);
        
        if (!missingTypes.isEmpty())
        {
            // if we don't have the atom's type locally, we need to fetch it first
            getRemoteTypes(getThisPeer().getIdentity(getSender(msg)), subgraph, missingTypes, typeMap);
            return WorkflowStateConstant.Started;
        }           
        readAtoms(subgraph, typeMap, null);
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
    
    public Map<HGHandle, Object> getAllAtoms()
    {
        return this.atomMap;
    }
    
    public Object getOneAtom()
    {
        return this.atomMap.values().iterator().next();
    }
}