package org.hypergraphdb.peer.cact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGAtomResolver;
import org.hypergraphdb.util.KeyMapResolver;
import org.hypergraphdb.util.MapResolver;
import org.hypergraphdb.util.Pair;

public class RunRemoteQuery extends FSMActivity
{
    public static final String TYPENAME = "run-query";
    
    private HGQueryCondition expression;
    private HGPeerIdentity target;
    private boolean deref = false;
    private List<Object> result = null;
    
    public RunRemoteQuery(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public RunRemoteQuery(HyperGraphPeer thisPeer, 
                          HGQueryCondition expression,
                          boolean deref,
                          HGPeerIdentity target)
    {
        super(thisPeer);
        this.target = target;
        this.deref = deref;
        this.expression = expression;
    }

    public String getType() { return TYPENAME; }
    
    public void initiate()
    {
        Message msg = createMessage(Performative.QueryRef,
                                    struct("condition", expression,
                                           "deref", deref));
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative = "QueryRef")
    public WorkflowStateConstant onQuery(Message msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        expression = getPart(msg, CONTENT, "condition");
        deref = getPart(msg, CONTENT, "deref");
        List<?> L = deref ? hg.getAll(graph, expression)
                          : hg.findAll(graph, expression);
        List<Object> result = new ArrayList<Object>();
        for (Object x : L)
        {
            if (x instanceof HGHandle)
                result.add(x);
            else
            {
                HGHandle atomHandle = graph.getHandle(x);
                if (atomHandle != null)
                    x = struct("atom-handle", atomHandle, 
                               "atom", SubgraphManager.getTransferAtomRepresentation(graph, atomHandle));
                result.add(x);                    
            }
        }
        reply(msg, 
              Performative.InformRef, result);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative = "InformRef")
    public WorkflowStateConstant onResponse(Message msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        List<?> L = getPart(msg, CONTENT);
        result = new ArrayList<Object>();
        for (Object x : L)
        {
            if (x instanceof HGHandle)
                result.add(x);
            else if (x instanceof Map)
            {
                HGHandle handle = getPart(x, "atom-handle");
                StorageGraph sgraph = getPart(x, "atom", "storage-graph");
                Map<String, String> typeClasses = getPart(x, "atom", "type-classes");    
                final Map<HGHandle, HGHandle> typeMap = 
                    SubgraphManager.getLocalTypes(graph, typeClasses);
                if (handle != null)
                {
                    result.add(new Pair<HGHandle, Object>(handle,
                            SubgraphManager.readAtom(handle, 
                                                     graph, 
                                                     new KeyMapResolver<HGHandle, HGAtomType>
                                                        (new MapResolver<HGHandle, HGHandle>(typeMap), 
                                                         new HGAtomResolver<HGAtomType>(graph)), 
                                                     sgraph)));
                }
                else
                    result.add(x);
            }
            else
                result.add(x);
        }
        return WorkflowState.Completed;
    }

    public HGQueryCondition getExpression()
    {
        return expression;
    }

    public void setExpression(HGQueryCondition expression)
    {
        this.expression = expression;
    }

    public HGPeerIdentity getTarget()
    {
        return target;
    }

    public void setTarget(HGPeerIdentity target)
    {
        this.target = target;
    }

    public boolean isDeref()
    {
        return deref;
    }

    public void setDeref(boolean deref)
    {
        this.deref = deref;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getResult()
    {
        return (List<T>)result;
    }

    @SuppressWarnings("unchecked")
    public <T> void setResult(List<T> result)
    {
        this.result = (List<Object>)result;
    }    
}
