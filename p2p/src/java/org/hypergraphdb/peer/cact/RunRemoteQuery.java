package org.hypergraphdb.peer.cact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.hypergraphdb.peer.Messages.*;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
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
    private Boolean deref = false;
    private Integer limit = -1; 
    private List<Object> result = null;
    
    public RunRemoteQuery(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public RunRemoteQuery(HyperGraphPeer thisPeer, 
                          HGQueryCondition expression,
                          boolean deref,
                          int limit,
                          HGPeerIdentity target)
    {
        super(thisPeer);
        this.target = target;
        this.deref = deref;
        this.limit = limit;
        this.expression = expression;
    }

    public String getType() { return TYPENAME; }
    
    public void initiate()
    {
    	Json msg = createMessage(Performative.QueryRef,
    					Json.object("condition", expression,
                                    "deref", deref));
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative = "QueryRef")
    public WorkflowStateConstant onQuery(Json msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        expression = Messages.fromJson(msg.at(CONTENT).at("condition"));
        deref = msg.at(CONTENT).at("deref").asBoolean();
        List<Object> L = null;
        switch (limit)
        {
            case 1:
                (L = new ArrayList<Object>()).add(deref ? hg.getOne(graph, expression) :
                                                          hg.findOne(graph, expression));
                break;
            case 0:
                L = new ArrayList<Object>();
                break;
            case -1:
                L = deref ? hg.getAll(graph, expression)
                          : hg.findAll(graph, expression);
                break;
            default:
                L = deref ? hg.getAll(graph, expression)
                        : hg.findAll(graph, expression);
                L = L.subList(0, limit);
        }
             
        Json result = Json.array();
        for (Object x : L)
        {
            if (x instanceof HGHandle)
                result.add(x);
            else
            {
                HGHandle atomHandle = graph.getHandle(x);
                if (atomHandle != null)
                    x = Json.object("atom-handle", atomHandle, 
                               		"atom", SubgraphManager.getTransferAtomRepresentation(graph, atomHandle));
                result.add(x);                    
            }
        }
        reply(msg, 
              Performative.InformRef, 
              result);
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative = "InformRef")
    public WorkflowStateConstant onResponse(Json msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        Json L = msg.at(CONTENT);
        result = new ArrayList<Object>();
        for (Json j : L.asJsonList())
        {
        	Object x = Messages.fromJson(j); 
            if (x instanceof HGHandle)
                result.add(x);
            else if (j.has("atom-handle"))
            {
                HGHandle handle = Messages.fromJson(j.at("atom-handle"));
                StorageGraph sgraph = SubgraphManager.decodeSubgraph(j.at("atom").at("storage-graph").asString());
                Map<String, String> typeClasses = Messages.fromJson(j.at("atom").at("type-classes"));    
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
    
    public int getLimit()
    {
        return limit;
    }

    public void setLimit(int limit)
    {
        this.limit = limit;
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