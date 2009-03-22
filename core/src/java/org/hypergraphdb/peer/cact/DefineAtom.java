package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Used to store an atom at a target peer.
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
        StorageGraph atomGraph = getThisPeer().getSubgraph(atom);
        Map<String, String> types = new HashMap<String, String>();
        for (Pair<HGPersistentHandle, Object> p : atomGraph)
        {
            String clname = getThisPeer().getGraph().getTypeSystem().getClassNameForType(p.getFirst());
            if (clname != null)
                types.put(p.getFirst().toString(), clname);
        }
        combine(msg, struct(CONTENT, 
                            struct("storage-graph", object(getThisPeer().getSubgraph(atom)),
                                   "type-classes", types)));
        send(target, msg);        
    }

    @FromState("Started")
    @OnMessage(performative="Request")
    @PossibleOutcome("Completed")    
    public WorkflowStateConstant onRequestDefine(Message msg) throws Throwable
    {
        final HyperGraph graph = getThisPeer().getGraph();
        final StorageGraph subgraph = getPart(msg, CONTENT, "storage-graph");
        final Map<String, String>  typeClasses = getPart(msg, CONTENT, "type-classes");
        final Map<HGPersistentHandle, HGPersistentHandle> substituteTypes = 
            new HashMap<HGPersistentHandle, HGPersistentHandle>();
        for (Map.Entry<String, String> e : typeClasses.entrySet())
        {
            HGPersistentHandle typeHandle = HGHandleFactory.makeHandle(e.getKey());
            String className = e.getValue();
            if (graph.get(typeHandle) == null) // do we have the atom type locally?
            {
                    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
                    HGHandle localType = graph.getTypeSystem().getTypeHandle(clazz);
                    if (localType == null)
                        throw new Exception("Unable to create local type for Java class '" + className + "'");
                    substituteTypes.put(typeHandle, graph.getPersistentHandle(localType));
            }
        }
        // TODO - here we assume that the types don't differ, but obviously they can
        // and will in many cases. So this is a major "TDB".        
        // If something goes wrong during storing the graph and reading back
        // an atom, the following will just throw an exception and the framework
        // will reply with failure.
        graph.getTransactionManager().transact(new Callable<Object>()
        {
            public Object call()
            {
                SubgraphManager.store(subgraph, graph.getStore(), substituteTypes);
                // TODO: read back the atom and refresh in cache if already cached!
                return null;
            }
        });
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