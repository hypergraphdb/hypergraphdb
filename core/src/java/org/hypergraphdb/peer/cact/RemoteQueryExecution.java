package org.hypergraphdb.peer.cact;

import static org.hypergraphdb.peer.Messages.CONTENT;
import static org.hypergraphdb.peer.Messages.getReply;
import static org.hypergraphdb.peer.Messages.getSender;
import static org.hypergraphdb.peer.Structs.combine;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.struct;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.workflow.ActivityResult;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.transaction.DefaultTransactionContext;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.HGTransactionContext;
import org.hypergraphdb.util.HGUtils;

public class RemoteQueryExecution<T> extends FSMActivity
{
    public static final String TYPENAME = "remote-query-execution";
    public static final WorkflowStateConstant ResultSetOpen = 
        WorkflowState.makeStateConstant("ResultSetOpen");
    private HGPeerIdentity target;
    private HGTransaction tx;
    private HGTransactionContext txContext;
    private HGQueryCondition queryExpression;
    private HGSearchResult<T> rs;

    private void bindTxContext()
    {
        if (txContext == null)
            txContext = new DefaultTransactionContext(getThisPeer().getGraph().getTransactionManager());
        getThisPeer().getGraph().getTransactionManager().threadAttach(txContext);
    }
    
    private void unbindTxContext()
    {
        getThisPeer().getGraph().getTransactionManager().threadDetach();
    }
    
    private class RemoteSearchResult implements HGSearchResult<T>
    {
        Boolean hasnext = false, hasprev = false;
        Boolean isordered = false;
        T current;

        public RemoteSearchResult(boolean hasnext, boolean isordered)
        {
            this.hasnext = hasnext;
            this.isordered = isordered;
        }

        public boolean hasPrev()
        {
            return hasprev;
        }

        public T prev()
        {
            Future<ActivityResult> f = getThisPeer().getActivityManager()
                    .initiateActivity(new IterateActivity<T>(getThisPeer(), "prev"),
                                      RemoteQueryExecution.this,
                                      null);
            try
            {
                ActivityResult result = f.get();
                if (result.getException() != null)
                    throw result.getException();
            }
            catch (Throwable e)
            {
                RemoteQueryExecution.this.getState().setFailed();
                throw new HGException(e);
            }
            return current;
        }

        public boolean hasNext()
        {
            return hasnext;
        }

        public T next()
        {
            Future<ActivityResult> f = getThisPeer().getActivityManager()
                    .initiateActivity(new IterateActivity<T>(getThisPeer(), "next"), 
                                      RemoteQueryExecution.this,
                                      null);
            try
            {
                ActivityResult result = f.get();
                if (result.getException() != null)
                    throw result.getException();
            }
            catch (Throwable e)
            {
                RemoteQueryExecution.this.getState().setFailed();
                throw new HGException(e);
            }
            return current;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public T current()
        {
            return current;
        }

        public void close()
        {
            send(target, createMessage(Performative.Cancel, RemoteQueryExecution.this));
            try
            {
                RemoteQueryExecution.this.getState().getFuture(WorkflowState.Completed).get();
            }
            catch (Exception ex)
            {
                throw new HGException(ex);
            }
        }

        public boolean isOrdered()
        {
            return isordered;
        }

    }

    // sub-activity to perform next/prev operations on the result set
    public static class IterateActivity<E> extends FSMActivity
    {
        public static final String TYPENAME = "remote-iterate-activity";
        
        String op;

        @SuppressWarnings("unchecked")
        RemoteQueryExecution<E> getParent() 
        {
            RemoteQueryExecution<E> p = (RemoteQueryExecution<E>)this.getThisPeer().getActivityManager().getParent(this);
            if (p == null)
                throw new NullPointerException("Remote IterateActivity without a parent.");
            return p;
        }
        
        public IterateActivity(HyperGraphPeer thisPeer, UUID id)
        {
            super(thisPeer, id);
        }

        public IterateActivity(HyperGraphPeer thisPeer, String op)
        {
            super(thisPeer);
            this.op = op;
        }

        public String getType () { return TYPENAME; }
        
        public void initiate()
        {
            Message msg = createMessage(Performative.QueryRef, op);
            send(getParent().target, msg);
        }

        @FromState("Started")
        @OnMessage(performative = "QueryRef")
        public WorkflowStateConstant onResultIterate(Message msg)
                throws Throwable
        {
            HyperGraph graph = getThisPeer().getGraph();
            getParent().bindTxContext();
            try
            {
                String op = getPart(msg, CONTENT);
                Message reply;
                if ("next".equals(op) || "prev".equals(op))
                {
                    Object x = "next".equals(op) ? getParent().rs.next() : getParent().rs.prev();
                    Object current = (x instanceof HGHandle) ? x
                            : SubgraphManager.getTransferAtomRepresentation(graph,
                                                                            graph.getHandle(x));
                    Boolean hasnext = getParent().rs.hasNext();
                    Boolean hasprev = false;
                    try { hasprev = getParent().rs.hasPrev(); }
                    catch (UnsupportedOperationException ex) { } // traversals, for example, don't support prev even though they should...
                    reply = getReply(msg, Performative.InformRef);
                    combine(reply,
                            struct(CONTENT,
                                   struct("has-next",
                                          hasnext,
                                          "has-prev",
                                          hasprev,
                                          "current",
                                          current)));
                }
                else
                    reply = getReply(msg, Performative.NotUnderstood);
                send(getSender(msg), reply);
                return WorkflowStateConstant.Completed;
            }
            finally
            {
                getParent().unbindTxContext();
            }
        }

        @SuppressWarnings("unchecked")
		@FromState("Started")
        @OnMessage(performative = "InformRef")
        public WorkflowStateConstant onIterateResult(Message msg)
                throws Throwable
        {
            Map<String, Object> C = getPart(msg, CONTENT);
            RemoteQueryExecution.RemoteSearchResult rs = 
            	(RemoteQueryExecution.RemoteSearchResult)getParent().rs;
            rs.hasnext = getPart(C, "has-next");
            rs.hasprev = getPart(C, "has-prev");
            rs.current = getPart(C, "current");
            return WorkflowStateConstant.Completed;
        }
    }

    public RemoteQueryExecution(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }
    
    public RemoteQueryExecution(HyperGraphPeer thisPeer, 
                                HGQueryCondition expression,
                                HGPeerIdentity target)
    {
        super(thisPeer);
        this.target = target;
        this.queryExpression = expression;
    }

    public void initiate()
    {
        Message msg = createMessage(Performative.Request, this);
        combine(msg, struct(CONTENT, queryExpression));
        send(target, msg);

    }

    @FromState("Started")
    @OnMessage(performative = "Request")
    public WorkflowStateConstant onQuery(Message msg) throws Throwable
    {
        queryExpression = getPart(msg, CONTENT);
        HyperGraph graph = getThisPeer().getGraph();
        bindTxContext();
        try
        {
            graph.getTransactionManager().beginTransaction(HGTransactionConfig.READONLY);
            tx = graph.getTransactionManager().getContext().getCurrent();
            rs = getThisPeer().getGraph().find(queryExpression);
            Message reply = getReply(msg, Performative.Agree);
            combine(reply,
                    struct(CONTENT,
                           struct("has-next",
                                  rs.hasNext(),
                                  "is-ordered",
                                  rs.isOrdered())));
            send(getSender(msg), reply);
            return ResultSetOpen;
        }
        finally
        {
            unbindTxContext();
        }
    }

    @FromState("Started")
    @OnMessage(performative = "Agree")
    public WorkflowStateConstant onQueryPerformed(Message msg) throws Throwable
    {
        Boolean hasnext = getPart(msg, CONTENT, "has-next");
        Boolean isordered = getPart(msg, CONTENT, "is-ordered");
        rs = new RemoteSearchResult(hasnext, isordered);
        return ResultSetOpen;
    }

    @FromState("ResultSetOpen")
    @OnMessage(performative = "Cancel")
    @PossibleOutcome("Completed")
    public WorkflowStateConstant onClose(Message msg) throws Throwable
    {
        bindTxContext();
        try
        {
            HGUtils.closeNoException(rs);
            tx.commit();
            reply(msg, Performative.Confirm, null);
            return WorkflowStateConstant.Completed;
        }
        finally
        {
            unbindTxContext();
        }
    }

    @FromState("ResultSetOpen")
    @OnMessage(performative = "Confirm")
    public WorkflowStateConstant onClosed(Message msg)
    {
        return WorkflowStateConstant.Completed;
    }
    
    public HGSearchResult<T> getSearchResult()
    {
        return rs;
    }

    public String getType()
    {
        return TYPENAME;
    }
}