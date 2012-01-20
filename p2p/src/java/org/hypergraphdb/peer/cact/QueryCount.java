package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.UUID;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.query.HGQueryCondition;

public class QueryCount extends FSMActivity
{
    public static final String TYPENAME = "query-count";
    
    private HGQueryCondition expression;
    private HGPeerIdentity target;
    private Long result = -1l;
    
    public QueryCount(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    public QueryCount(HyperGraphPeer thisPeer, 
                      HGQueryCondition expression,
                      HGPeerIdentity target)
    {
        super(thisPeer);
        this.target = target;
        this.expression = expression;
    }

    public String getType() { return TYPENAME; }
    
    public void initiate()
    {
        Message msg = createMessage(Performative.QueryRef,
                                    struct("condition", expression));
        send(target, msg);
    }

    @FromState("Started")
    @OnMessage(performative = "QueryRef")
    public WorkflowStateConstant onQuery(Message msg)
    {
        HyperGraph graph = getThisPeer().getGraph();
        expression = getPart(msg, CONTENT, "condition");
        reply(msg, 
              Performative.InformRef, hg.count(graph, expression));
        return WorkflowState.Completed;
    }
    
    @FromState("Started")
    @OnMessage(performative = "InformRef")
    public WorkflowStateConstant onResponse(Message msg)
    {
        result = getPart(msg, CONTENT);
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

    public long getResult()
    {
        return result;
    }
}
